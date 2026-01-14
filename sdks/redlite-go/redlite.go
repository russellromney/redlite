// Package redlite provides a Redis-compatible client with redlite-specific extensions.
//
// It wraps go-redis/v9 and adds namespaces for full-text search, history tracking,
// vector search, and geospatial commands.
//
// Example usage:
//
//	// Embedded mode - start internal server
//	db, err := redlite.Open("mydata.db")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	// Server mode - connect to running server
//	db, err := redlite.Connect("localhost:6379")
//
//	// Use Redis commands
//	db.Set(ctx, "key", "value", 0)
//	val, _ := db.Get(ctx, "key").Result()
//
//	// Redlite-specific namespaces
//	db.FTS.Enable(ctx, FTSEnableGlobal())
//	db.History.Enable(ctx, HistoryEnableGlobal())
//	db.Vector.Add(ctx, "embeddings", "doc1", []float64{0.1, 0.2, 0.3})
package redlite

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redlite is a Redis-compatible client with redlite-specific extensions.
type Redlite struct {
	*redis.Client

	// FTS provides full-text search commands
	FTS *FTSNamespace

	// FT provides RediSearch-compatible FT.* commands
	FT *FTNamespace

	// History provides version history tracking
	History *HistoryNamespace

	// Vector provides vector similarity search
	Vector *VectorNamespace

	// Geo provides geospatial commands with a Pythonic API
	Geo *GeoNamespace

	// embedded server management
	embeddedCmd  *exec.Cmd
	embeddedPort int
}

// Connect connects to a running redlite or Redis server.
//
// URL formats:
//   - "localhost:6379" (host:port)
//   - "redis://host:6379" (Redis URL)
//   - "redis://user:pass@host:6379/0" (with auth and db)
func Connect(url string) (*Redlite, error) {
	var opts *redis.Options
	var err error

	if strings.HasPrefix(url, "redis://") || strings.HasPrefix(url, "rediss://") {
		opts, err = redis.ParseURL(url)
		if err != nil {
			return nil, fmt.Errorf("invalid redis URL: %w", err)
		}
	} else {
		// Simple host:port format
		host := url
		port := 6379
		if idx := strings.LastIndex(url, ":"); idx != -1 {
			host = url[:idx]
			port, err = strconv.Atoi(url[idx+1:])
			if err != nil {
				return nil, fmt.Errorf("invalid port in URL: %w", err)
			}
		}
		opts = &redis.Options{
			Addr: fmt.Sprintf("%s:%d", host, port),
		}
	}

	client := redis.NewClient(opts)
	return newRedlite(client, nil, 0), nil
}

// Open opens a redlite database in embedded mode.
//
// Starts an internal redlite server process and connects via TCP.
//
// Path can be:
//   - A file path for persistent storage (e.g., "mydata.db")
//   - ":memory:" for in-memory database
func Open(path string, opts ...OpenOption) (*Redlite, error) {
	cfg := &openConfig{
		cacheMB:        64,
		startupTimeout: 5 * time.Second,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	// Find the redlite binary
	binary, err := findBinary(cfg.binaryPath)
	if err != nil {
		return nil, err
	}

	// Find a free port
	port, err := findFreePort()
	if err != nil {
		return nil, fmt.Errorf("failed to find free port: %w", err)
	}

	// Build command arguments
	args := []string{
		"--addr", fmt.Sprintf("127.0.0.1:%d", port),
		"--cache", strconv.Itoa(cfg.cacheMB),
	}

	if path == ":memory:" {
		args = append(args, "--storage", "memory")
	} else {
		args = append(args, "--db", path, "--storage", "file")
	}

	// Start the server
	cmd := exec.Command(binary, args...)
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start redlite server: %w", err)
	}

	// Wait for server to be ready
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	if err := waitForServer(addr, cfg.startupTimeout); err != nil {
		cmd.Process.Kill()
		return nil, err
	}

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	return newRedlite(client, cmd, port), nil
}

// OpenOption configures Open behavior.
type OpenOption func(*openConfig)

type openConfig struct {
	cacheMB        int
	binaryPath     string
	startupTimeout time.Duration
}

// WithCacheMB sets the SQLite cache size in megabytes.
func WithCacheMB(mb int) OpenOption {
	return func(c *openConfig) {
		c.cacheMB = mb
	}
}

// WithBinaryPath sets a custom path to the redlite binary.
func WithBinaryPath(path string) OpenOption {
	return func(c *openConfig) {
		c.binaryPath = path
	}
}

// WithStartupTimeout sets the timeout for server startup.
func WithStartupTimeout(d time.Duration) OpenOption {
	return func(c *openConfig) {
		c.startupTimeout = d
	}
}

// FromClient wraps an existing go-redis client.
func FromClient(client *redis.Client) *Redlite {
	return newRedlite(client, nil, 0)
}

func newRedlite(client *redis.Client, cmd *exec.Cmd, port int) *Redlite {
	r := &Redlite{
		Client:       client,
		embeddedCmd:  cmd,
		embeddedPort: port,
	}
	r.FTS = &FTSNamespace{client: r}
	r.FT = &FTNamespace{client: r}
	r.History = &HistoryNamespace{client: r}
	r.Vector = &VectorNamespace{client: r}
	r.Geo = &GeoNamespace{client: r}
	return r
}

// Close closes the connection and stops the embedded server if running.
func (r *Redlite) Close() error {
	err := r.Client.Close()

	if r.embeddedCmd != nil && r.embeddedCmd.Process != nil {
		r.embeddedCmd.Process.Kill()
		r.embeddedCmd.Wait()
		r.embeddedCmd = nil
	}

	return err
}

// -----------------------------------------------------------------------------
// Redlite-specific Commands
// -----------------------------------------------------------------------------

// Vacuum runs SQLite VACUUM to reclaim storage space.
func (r *Redlite) Vacuum(ctx context.Context) error {
	return r.Do(ctx, "VACUUM").Err()
}

// Autovacuum gets or sets the autovacuum setting.
func (r *Redlite) Autovacuum(ctx context.Context, enabled *bool) (string, error) {
	if enabled == nil {
		return r.Do(ctx, "AUTOVACUUM").Text()
	}
	val := "OFF"
	if *enabled {
		val = "ON"
	}
	return r.Do(ctx, "AUTOVACUUM", val).Text()
}

// KeyInfo returns detailed information about a key.
func (r *Redlite) KeyInfo(ctx context.Context, key string) (map[string]interface{}, error) {
	result, err := r.Do(ctx, "KEYINFO", key).Slice()
	if err != nil {
		return nil, err
	}
	return parseKeyValuePairs(result), nil
}

// -----------------------------------------------------------------------------
// Helper Functions
// -----------------------------------------------------------------------------

func findBinary(customPath string) (string, error) {
	// Custom path
	if customPath != "" {
		if _, err := os.Stat(customPath); err == nil {
			return customPath, nil
		}
		return "", fmt.Errorf("binary not found at specified path: %s", customPath)
	}

	// Environment variable
	if envBinary := os.Getenv("REDLITE_BINARY"); envBinary != "" {
		if _, err := os.Stat(envBinary); err == nil {
			return envBinary, nil
		}
	}

	// Try bundled binary
	execPath, err := os.Executable()
	if err == nil {
		bundledPath := filepath.Join(filepath.Dir(execPath), "redlite")
		if runtime.GOOS == "windows" {
			bundledPath += ".exe"
		}
		if _, err := os.Stat(bundledPath); err == nil {
			return bundledPath, nil
		}
	}

	// Try system PATH
	if path, err := exec.LookPath("redlite"); err == nil {
		return path, nil
	}

	// Try common locations
	var commonPaths []string
	switch runtime.GOOS {
	case "darwin":
		commonPaths = []string{
			"/usr/local/bin/redlite",
			"/opt/homebrew/bin/redlite",
			filepath.Join(os.Getenv("HOME"), ".cargo", "bin", "redlite"),
		}
	case "linux":
		commonPaths = []string{
			"/usr/local/bin/redlite",
			"/usr/bin/redlite",
			filepath.Join(os.Getenv("HOME"), ".cargo", "bin", "redlite"),
		}
	case "windows":
		commonPaths = []string{
			filepath.Join(os.Getenv("USERPROFILE"), ".cargo", "bin", "redlite.exe"),
		}
	}

	for _, p := range commonPaths {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}

	return "", fmt.Errorf("could not find redlite binary; install it or set REDLITE_BINARY")
}

func findFreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func waitForServer(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			// Try a PING to make sure Redis is ready
			client := redis.NewClient(&redis.Options{Addr: addr})
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			err := client.Ping(ctx).Err()
			cancel()
			client.Close()
			if err == nil {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for server at %s", addr)
}

func parseKeyValuePairs(slice []interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for i := 0; i < len(slice)-1; i += 2 {
		key, ok := slice[i].(string)
		if !ok {
			continue
		}
		result[key] = slice[i+1]
	}
	return result
}

func interfaceSliceToStrings(slice []interface{}) []string {
	result := make([]string, len(slice))
	for i, v := range slice {
		switch val := v.(type) {
		case string:
			result[i] = val
		case []byte:
			result[i] = string(val)
		default:
			result[i] = fmt.Sprintf("%v", val)
		}
	}
	return result
}
