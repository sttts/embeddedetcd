/*
Copyright 2026 The KCP Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package embeddedetcd

// Benchmarks to measure the performance impact of UNSAFE_E2E_HACK_DISABLE_ETCD_FSYNC optimizations.
//
// Run benchmarks with:
//   go test -bench=. -benchmem -benchtime=10s
//
// These benchmarks compare three modes:
//   1. BenchmarkNormalMode - Default disk-based operation with fsync enabled
//   2. BenchmarkUnsafeModeOld - UNSAFE_E2E_HACK_DISABLE_ETCD_FSYNC=true WITHOUT optimizations (MaxWalFiles/MaxSnapFiles not minimized)
//   3. BenchmarkUnsafeModeOptimized - UNSAFE_E2E_HACK_DISABLE_ETCD_FSYNC=true WITH WAL/snapshot minimizations (actual new behavior)

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/storage/wal"
)

// BenchmarkNormalMode benchmarks the default disk-based operation with fsync enabled
func BenchmarkNormalMode(b *testing.B) {
	runBenchmark(b, benchmarkConfig{
		name:           "NormalMode",
		unsafeNoFsync:  false,
		setMaxFiles:    false,
		walSizeBytes:   0,
	})
}

// BenchmarkUnsafeModeOld benchmarks UNSAFE_E2E_HACK_DISABLE_ETCD_FSYNC=true without the new optimizations
// This simulates the old behavior by NOT setting MaxWalFiles/MaxSnapFiles
func BenchmarkUnsafeModeOld(b *testing.B) {
	runBenchmark(b, benchmarkConfig{
		name:           "UnsafeModeOld",
		unsafeNoFsync:  true,
		setMaxFiles:    false, // Old behavior - don't minimize files
		walSizeBytes:   0,
	})
}

// BenchmarkUnsafeModeOptimized benchmarks UNSAFE_E2E_HACK_DISABLE_ETCD_FSYNC=true with the new optimizations
// This is the actual new behavior with WAL/snapshot minimizations
func BenchmarkUnsafeModeOptimized(b *testing.B) {
	runBenchmark(b, benchmarkConfig{
		name:           "UnsafeModeOptimized",
		unsafeNoFsync:  true,
		setMaxFiles:    true, // New behavior - minimize files
		walSizeBytes:   1 << 20, // 1MB
	})
}

type benchmarkConfig struct {
	name          string
	unsafeNoFsync bool
	setMaxFiles   bool
	walSizeBytes  int64
}

func runBenchmark(b *testing.B, cfg benchmarkConfig) {
	// Create temporary directory for this benchmark
	tmpDir := b.TempDir()
	
	// Save and restore original WAL segment size
	originalWalSize := wal.SegmentSizeBytes
	defer func() {
		wal.SegmentSizeBytes = originalWalSize
	}()
	
	// Configure WAL size
	if cfg.walSizeBytes > 0 {
		wal.SegmentSizeBytes = cfg.walSizeBytes
	} else {
		wal.SegmentSizeBytes = 64 * 1024 * 1024 // 64MB default
	}
	
	// Create etcd config
	etcdCfg := embed.NewConfig()
	etcdCfg.Logger = "zap"
	etcdCfg.LogLevel = "fatal" // Minimize log noise during benchmarks
	etcdCfg.Dir = filepath.Join(tmpDir, "etcd-data")
	
	// Apply unsafe mode settings
	etcdCfg.UnsafeNoFsync = cfg.unsafeNoFsync
	
	// Apply file minimization settings
	if cfg.setMaxFiles {
		etcdCfg.MaxWalFiles = 1
		etcdCfg.MaxSnapFiles = 1
	}
	
	// Start the etcd server
	e, err := embed.StartEtcd(etcdCfg)
	if err != nil {
		b.Fatalf("Failed to start etcd: %v", err)
	}
	defer e.Close()
	
	// Wait for server to be ready
	select {
	case <-e.Server.ReadyNotify():
		// Server is ready
	case <-time.After(60 * time.Second):
		b.Fatal("Server took too long to start")
	case err := <-e.Err():
		b.Fatalf("Server error: %v", err)
	}
	
	// Create etcd client
	clientCfg := clientv3.Config{
		Endpoints:   []string{etcdCfg.ListenClientUrls[0].String()},
		DialTimeout: 5 * time.Second,
	}
	client, err := clientv3.New(clientCfg)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()
	
	// Reset timer to exclude setup time
	b.ResetTimer()
	
	// Run the benchmark operations
	for i := 0; i < b.N; i++ {
		// Write a key
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		key := fmt.Sprintf("/benchmark/key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		_, err := client.Put(ctx, key, value)
		cancel()
		if err != nil {
			b.Fatalf("Failed to put key: %v", err)
		}
		
		// Read the key back
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.Get(ctx, key)
		cancel()
		if err != nil {
			b.Fatalf("Failed to get key: %v", err)
		}
		if len(resp.Kvs) != 1 {
			b.Fatalf("Expected 1 key, got %d", len(resp.Kvs))
		}
	}
	
	// Stop timer before measuring metrics
	b.StopTimer()
	
	// Measure disk usage and file counts
	diskUsedMB, walFiles, snapFiles := measureDiskMetrics(etcdCfg.Dir)
	
	// Report custom metrics
	b.ReportMetric(diskUsedMB, "diskUsedMB")
	b.ReportMetric(float64(walFiles), "walFileCount")
	b.ReportMetric(float64(snapFiles), "snapFileCount")
}

// measureDiskMetrics calculates disk usage and counts WAL and snapshot files
func measureDiskMetrics(dir string) (diskUsedMB float64, walFiles int, snapFiles int) {
	var totalBytes int64
	
	// Walk the directory tree and sum up file sizes
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			totalBytes += info.Size()
			
			// Count WAL files (*.wal files in member/wal directory)
			if filepath.Dir(path) == filepath.Join(dir, "member", "wal") && filepath.Ext(path) == ".wal" {
				walFiles++
			}
			
			// Count snapshot files (*.snap files in member/snap directory)
			if filepath.Dir(path) == filepath.Join(dir, "member", "snap") && filepath.Ext(path) == ".snap" {
				snapFiles++
			}
		}
		return nil
	})
	
	diskUsedMB = float64(totalBytes) / (1024 * 1024)
	return
}
