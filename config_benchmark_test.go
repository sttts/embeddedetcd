/*
Copyright 2022 The KCP Authors.

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

package embeddedetcd_test

// Benchmarks to measure performance impact of UNSAFE_E2E_HACK_DISABLE_ETCD_FSYNC optimizations.
//
// Run with:
//   go test -bench=. -benchmem -benchtime=10s
//
// For quick checks:
//   go test -bench=. -benchmem -short

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kcp-dev/embeddedetcd"
	"github.com/kcp-dev/embeddedetcd/options"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	numOperations        = 1000             // Number of key-value pairs to write/read
	keyPrefix            = "benchmark-key-"
	valuePrefix          = "benchmark-value-"
	serverStartupTimeout = 65 * time.Second // Timeout for etcd server startup (slightly longer than etcd's internal 60s timeout)
)

func BenchmarkNormalMode(b *testing.B) {
	benchmarkEtcdMode(b, false, false)
}

func BenchmarkUnsafeModeOld(b *testing.B) {
	benchmarkEtcdMode(b, true, false)
}

func BenchmarkUnsafeModeOptimized(b *testing.B) {
	benchmarkEtcdMode(b, true, true)
}

func benchmarkEtcdMode(b *testing.B, unsafeMode bool, withOptimizations bool) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Create temporary directory
		tmpDir, err := os.MkdirTemp("", "etcd-bench-*")
		if err != nil {
			b.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		// Set environment variable for unsafe mode
		oldEnv := os.Getenv("UNSAFE_E2E_HACK_DISABLE_ETCD_FSYNC")
		if unsafeMode {
			os.Setenv("UNSAFE_E2E_HACK_DISABLE_ETCD_FSYNC", "true")
		} else {
			os.Unsetenv("UNSAFE_E2E_HACK_DISABLE_ETCD_FSYNC")
		}
		defer func() {
			if oldEnv != "" {
				os.Setenv("UNSAFE_E2E_HACK_DISABLE_ETCD_FSYNC", oldEnv)
			} else {
				os.Unsetenv("UNSAFE_E2E_HACK_DISABLE_ETCD_FSYNC")
			}
		}()

		// Create options
		opts := options.NewOptions(tmpDir)
		opts.ClientPort = findAvailablePort()
		opts.PeerPort = findAvailablePort()

		completedOpts := opts.Complete(nil)

		// Create config
		cfg, err := embeddedetcd.NewConfig(completedOpts, false)
		if err != nil {
			b.Fatalf("Failed to create config: %v", err)
		}

		// Apply optimizations for optimized unsafe mode
		if withOptimizations {
			// Minimize snapshot creation
			cfg.SnapshotCount = 100000 // Very high value to minimize snapshots
			cfg.MaxSnapFiles = 1       // Keep only one snapshot file
			cfg.MaxWalFiles = 1        // Keep only one WAL file
		}

		completedCfg := cfg.Complete()

		// Create and start server
		server := embeddedetcd.NewServer(completedCfg)
		if server == nil {
			b.Fatalf("Failed to create server")
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start server in a goroutine and wait for it to be ready
		readyChan := make(chan error, 1)
		go func() {
			readyChan <- server.Run(ctx)
		}()

		// Wait for server to be ready (Run returns nil when ready)
		select {
		case err := <-readyChan:
			if err != nil {
				b.Fatalf("Server failed to start: %v", err)
			}
			// Server is ready (Run returned nil)
		case <-time.After(serverStartupTimeout):
			cancel()
			b.Fatalf("Server took too long to start")
		}

		// Create etcd client with proper TLS
		secretsDir := filepath.Join(tmpDir, "etcd-server", "secrets")
		caCert, err := os.ReadFile(filepath.Join(secretsDir, "ca", "cert.pem"))
		if err != nil {
			cancel()
			b.Fatalf("Failed to read CA cert: %v", err)
		}
		clientCert, err := tls.LoadX509KeyPair(
			filepath.Join(secretsDir, "client", "cert.pem"),
			filepath.Join(secretsDir, "client", "key.pem"),
		)
		if err != nil {
			cancel()
			b.Fatalf("Failed to load client cert: %v", err)
		}

		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(caCert)

		clientCfg := clientv3.Config{
			Endpoints:   []string{fmt.Sprintf("https://localhost:%s", opts.ClientPort)},
			DialTimeout: 5 * time.Second,
			TLS: &tls.Config{
				RootCAs:      certPool,
				Certificates: []tls.Certificate{clientCert},
			},
		}

		client, err := clientv3.New(clientCfg)
		if err != nil {
			cancel()
			b.Fatalf("Failed to create client: %v", err)
		}
		defer client.Close()

		b.StartTimer()

		// Perform write operations
		for j := 0; j < numOperations; j++ {
			key := fmt.Sprintf("%s%d", keyPrefix, j)
			value := fmt.Sprintf("%s%d", valuePrefix, j)
			_, err := client.Put(context.Background(), key, value)
			if err != nil {
				b.Fatalf("Failed to put key %s: %v", key, err)
			}
		}

		// Perform read operations
		for j := 0; j < numOperations; j++ {
			key := fmt.Sprintf("%s%d", keyPrefix, j)
			resp, err := client.Get(context.Background(), key)
			if err != nil {
				b.Fatalf("Failed to get key %s: %v", key, err)
			}
			if len(resp.Kvs) == 0 {
				b.Fatalf("Key %s not found", key)
			}
		}

		b.StopTimer()

		// Measure disk usage and file counts
		diskUsed, walFiles, snapFiles := measureDiskUsage(tmpDir)

		// Report custom metrics
		b.ReportMetric(float64(diskUsed)/(1024*1024), "diskUsedMB")
		b.ReportMetric(float64(walFiles), "walFileCount")
		b.ReportMetric(float64(snapFiles), "snapFileCount")

		// Stop the server
		cancel()

		// Wait for server to shut down
		time.Sleep(1 * time.Second)
	}
}

// findAvailablePort returns a random available port number as a string
// Note: This simple port allocation strategy works for sequential benchmark runs.
// For parallel benchmarks, consider using a more sophisticated approach such as
// binding to port 0 and retrieving the OS-assigned port.
func findAvailablePort() string {
	// Use a simple port allocation strategy based on process ID and timestamp
	// This should provide unique ports for sequential benchmark runs
	base := 22379
	offset := (os.Getpid() + int(time.Now().UnixNano()%10000)) % 10000
	port := base + offset
	return fmt.Sprintf("%d", port)
}

// measureDiskUsage calculates the total disk usage, WAL file count, and snapshot file count
func measureDiskUsage(dir string) (totalBytes int64, walFiles int, snapFiles int) {
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			info, err := d.Info()
			if err == nil {
				totalBytes += info.Size()

				// Count WAL files (*.wal extension)
				if filepath.Ext(path) == ".wal" {
					walFiles++
				}
				// Count snapshot files (*.snap extension OR files in snap/ directory)
				// etcd stores snapshots with .snap extension in the snap/ subdirectory
				if filepath.Ext(path) == ".snap" || filepath.Base(filepath.Dir(path)) == "snap" {
					snapFiles++
				}
			}
		}
		return nil
	})
	if err != nil {
		// Log error but don't fail the benchmark
		// In worst case, metrics will be zero
		return 0, 0, 0
	}
	return
}
