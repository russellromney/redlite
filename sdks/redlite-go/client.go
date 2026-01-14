// Package redlite provides unified embedded and server mode Redis-compatible clients.
//
// Unified API:
//
//	// Embedded mode (FFI, no network, microsecond latency)
//	db, err := redlite.New(":memory:")
//	db, err := redlite.New("/path/to/db.db")
//
//	// Server mode (wraps go-redis)
//	db, err := redlite.New("redis://localhost:6379")
//
//	// Use the same API either way
//	db.Set(ctx, "key", "value", 0)
//	val, _ := db.Get(ctx, "key")
//
// For power users needing direct access:
//
//	// Direct FFI (embedded only, no context)
//	embedded, _ := redlite.OpenEmbedded(":memory:")
//	embedded.Set("key", []byte("value"), 0)
//
//	// Direct go-redis wrapper (server only)
//	server, _ := redlite.Connect("redis://localhost:6379")
//	server.Set(ctx, "key", "value", 0)
package redlite

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client is a unified interface for both embedded and server modes.
// It provides a subset of Redis commands that work identically in both modes.
type Client interface {
	// Close closes the database connection.
	Close() error

	// Mode returns "embedded" or "server".
	Mode() string

	// String commands
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, expiration time.Duration) error
	Del(ctx context.Context, keys ...string) (int64, error)
	Exists(ctx context.Context, keys ...string) (int64, error)
	Incr(ctx context.Context, key string) (int64, error)
	Decr(ctx context.Context, key string) (int64, error)

	// TTL commands
	TTL(ctx context.Context, key string) (int64, error)
	Expire(ctx context.Context, key string, seconds int64) (bool, error)

	// Hash commands
	HSet(ctx context.Context, key string, fields map[string][]byte) (int64, error)
	HGet(ctx context.Context, key, field string) ([]byte, error)
	HDel(ctx context.Context, key string, fields ...string) (int64, error)
	HLen(ctx context.Context, key string) (int64, error)

	// List commands
	LPush(ctx context.Context, key string, values ...[]byte) (int64, error)
	RPush(ctx context.Context, key string, values ...[]byte) (int64, error)
	LPop(ctx context.Context, key string) ([]byte, error)
	RPop(ctx context.Context, key string) ([]byte, error)
	LLen(ctx context.Context, key string) (int64, error)
	LRange(ctx context.Context, key string, start, stop int64) ([][]byte, error)

	// Set commands
	SAdd(ctx context.Context, key string, members ...[]byte) (int64, error)
	SRem(ctx context.Context, key string, members ...[]byte) (int64, error)
	SMembers(ctx context.Context, key string) ([][]byte, error)
	SIsMember(ctx context.Context, key string, member []byte) (bool, error)
	SCard(ctx context.Context, key string) (int64, error)

	// Sorted set commands
	ZAdd(ctx context.Context, key string, members ...ZMemberScore) (int64, error)
	ZScore(ctx context.Context, key string, member []byte) (float64, bool, error)
	ZCard(ctx context.Context, key string) (int64, error)

	// Server commands
	DBSize(ctx context.Context) (int64, error)
	FlushDB(ctx context.Context) error

	// Execute raw command (for Redlite-specific commands like FT.*, V.*, etc.)
	Do(ctx context.Context, args ...interface{}) (interface{}, error)
}

// Option configures New() behavior.
type Option func(*clientConfig)

type clientConfig struct {
	cacheMB        int
	startupTimeout time.Duration
}

// WithCache sets the SQLite cache size in MB (embedded mode only).
func WithCache(mb int) Option {
	return func(c *clientConfig) {
		c.cacheMB = mb
	}
}

// New creates a unified client that auto-detects mode from URL.
//
// URL formats:
//   - ":memory:" for in-memory embedded database
//   - "/path/to/db.db" for file-based embedded database
//   - "redis://host:port" for server mode
//   - "rediss://host:port" for TLS server mode
func New(url string, opts ...Option) (Client, error) {
	cfg := &clientConfig{
		cacheMB:        64,
		startupTimeout: 5 * time.Second,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	if strings.HasPrefix(url, "redis://") || strings.HasPrefix(url, "rediss://") {
		// Server mode - wrap go-redis via existing Redlite
		r, err := Connect(url)
		if err != nil {
			return nil, err
		}
		return &serverClient{r}, nil
	}

	// Embedded mode - use FFI
	db, err := OpenEmbeddedWithCache(url, int64(cfg.cacheMB))
	if err != nil {
		return nil, err
	}
	return &embeddedClient{db}, nil
}

// embeddedClient wraps EmbeddedDb to implement Client interface.
type embeddedClient struct {
	db *EmbeddedDb
}

func (c *embeddedClient) Close() error {
	return c.db.Close()
}

func (c *embeddedClient) Mode() string {
	return "embedded"
}

func (c *embeddedClient) Get(ctx context.Context, key string) ([]byte, error) {
	return c.db.Get(key)
}

func (c *embeddedClient) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	return c.db.Set(key, value, expiration)
}

func (c *embeddedClient) Del(ctx context.Context, keys ...string) (int64, error) {
	return c.db.Del(keys...)
}

func (c *embeddedClient) Exists(ctx context.Context, keys ...string) (int64, error) {
	return c.db.Exists(keys...)
}

func (c *embeddedClient) Incr(ctx context.Context, key string) (int64, error) {
	return c.db.Incr(key)
}

func (c *embeddedClient) Decr(ctx context.Context, key string) (int64, error) {
	return c.db.Decr(key)
}

func (c *embeddedClient) TTL(ctx context.Context, key string) (int64, error) {
	return c.db.TTL(key)
}

func (c *embeddedClient) Expire(ctx context.Context, key string, seconds int64) (bool, error) {
	return c.db.Expire(key, seconds)
}

func (c *embeddedClient) HSet(ctx context.Context, key string, fields map[string][]byte) (int64, error) {
	return c.db.HSet(key, fields)
}

func (c *embeddedClient) HGet(ctx context.Context, key, field string) ([]byte, error) {
	return c.db.HGet(key, field)
}

func (c *embeddedClient) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	return c.db.HDel(key, fields...)
}

func (c *embeddedClient) HLen(ctx context.Context, key string) (int64, error) {
	return c.db.HLen(key)
}

func (c *embeddedClient) LPush(ctx context.Context, key string, values ...[]byte) (int64, error) {
	return c.db.LPush(key, values...)
}

func (c *embeddedClient) RPush(ctx context.Context, key string, values ...[]byte) (int64, error) {
	return c.db.RPush(key, values...)
}

func (c *embeddedClient) LPop(ctx context.Context, key string) ([]byte, error) {
	items, err := c.db.LPop(key, 1)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, nil
	}
	return items[0], nil
}

func (c *embeddedClient) RPop(ctx context.Context, key string) ([]byte, error) {
	items, err := c.db.RPop(key, 1)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, nil
	}
	return items[0], nil
}

func (c *embeddedClient) LLen(ctx context.Context, key string) (int64, error) {
	return c.db.LLen(key)
}

func (c *embeddedClient) LRange(ctx context.Context, key string, start, stop int64) ([][]byte, error) {
	return c.db.LRange(key, start, stop)
}

func (c *embeddedClient) SAdd(ctx context.Context, key string, members ...[]byte) (int64, error) {
	return c.db.SAdd(key, members...)
}

func (c *embeddedClient) SRem(ctx context.Context, key string, members ...[]byte) (int64, error) {
	return c.db.SRem(key, members...)
}

func (c *embeddedClient) SMembers(ctx context.Context, key string) ([][]byte, error) {
	return c.db.SMembers(key)
}

func (c *embeddedClient) SIsMember(ctx context.Context, key string, member []byte) (bool, error) {
	return c.db.SIsMember(key, member)
}

func (c *embeddedClient) SCard(ctx context.Context, key string) (int64, error) {
	return c.db.SCard(key)
}

func (c *embeddedClient) ZAdd(ctx context.Context, key string, members ...ZMemberScore) (int64, error) {
	return c.db.ZAdd(key, members...)
}

func (c *embeddedClient) ZScore(ctx context.Context, key string, member []byte) (float64, bool, error) {
	return c.db.ZScore(key, member)
}

func (c *embeddedClient) ZCard(ctx context.Context, key string) (int64, error) {
	return c.db.ZCard(key)
}

func (c *embeddedClient) DBSize(ctx context.Context) (int64, error) {
	return c.db.DBSize()
}

func (c *embeddedClient) FlushDB(ctx context.Context) error {
	return c.db.FlushDB()
}

func (c *embeddedClient) Do(ctx context.Context, args ...interface{}) (interface{}, error) {
	// Embedded mode doesn't support arbitrary commands via FFI
	// Return an error for now
	return nil, ErrClosed // TODO: implement command dispatcher
}

// serverClient wraps Redlite to implement Client interface.
type serverClient struct {
	r *Redlite
}

func (c *serverClient) Close() error {
	return c.r.Close()
}

func (c *serverClient) Mode() string {
	return "server"
}

func (c *serverClient) Get(ctx context.Context, key string) ([]byte, error) {
	return c.r.Client.Get(ctx, key).Bytes()
}

func (c *serverClient) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	return c.r.Client.Set(ctx, key, value, expiration).Err()
}

func (c *serverClient) Del(ctx context.Context, keys ...string) (int64, error) {
	return c.r.Client.Del(ctx, keys...).Result()
}

func (c *serverClient) Exists(ctx context.Context, keys ...string) (int64, error) {
	return c.r.Client.Exists(ctx, keys...).Result()
}

func (c *serverClient) Incr(ctx context.Context, key string) (int64, error) {
	return c.r.Client.Incr(ctx, key).Result()
}

func (c *serverClient) Decr(ctx context.Context, key string) (int64, error) {
	return c.r.Client.Decr(ctx, key).Result()
}

func (c *serverClient) TTL(ctx context.Context, key string) (int64, error) {
	d, err := c.r.Client.TTL(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	return int64(d.Seconds()), nil
}

func (c *serverClient) Expire(ctx context.Context, key string, seconds int64) (bool, error) {
	return c.r.Client.Expire(ctx, key, time.Duration(seconds)*time.Second).Result()
}

func (c *serverClient) HSet(ctx context.Context, key string, fields map[string][]byte) (int64, error) {
	// Convert map[string][]byte to []interface{} for HSet
	args := make([]interface{}, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}
	return c.r.Client.HSet(ctx, key, args...).Result()
}

func (c *serverClient) HGet(ctx context.Context, key, field string) ([]byte, error) {
	return c.r.Client.HGet(ctx, key, field).Bytes()
}

func (c *serverClient) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	return c.r.Client.HDel(ctx, key, fields...).Result()
}

func (c *serverClient) HLen(ctx context.Context, key string) (int64, error) {
	return c.r.Client.HLen(ctx, key).Result()
}

func (c *serverClient) LPush(ctx context.Context, key string, values ...[]byte) (int64, error) {
	args := make([]interface{}, len(values))
	for i, v := range values {
		args[i] = v
	}
	return c.r.Client.LPush(ctx, key, args...).Result()
}

func (c *serverClient) RPush(ctx context.Context, key string, values ...[]byte) (int64, error) {
	args := make([]interface{}, len(values))
	for i, v := range values {
		args[i] = v
	}
	return c.r.Client.RPush(ctx, key, args...).Result()
}

func (c *serverClient) LPop(ctx context.Context, key string) ([]byte, error) {
	return c.r.Client.LPop(ctx, key).Bytes()
}

func (c *serverClient) RPop(ctx context.Context, key string) ([]byte, error) {
	return c.r.Client.RPop(ctx, key).Bytes()
}

func (c *serverClient) LLen(ctx context.Context, key string) (int64, error) {
	return c.r.Client.LLen(ctx, key).Result()
}

func (c *serverClient) LRange(ctx context.Context, key string, start, stop int64) ([][]byte, error) {
	result, err := c.r.Client.LRange(ctx, key, start, stop).Result()
	if err != nil {
		return nil, err
	}
	bytes := make([][]byte, len(result))
	for i, s := range result {
		bytes[i] = []byte(s)
	}
	return bytes, nil
}

func (c *serverClient) SAdd(ctx context.Context, key string, members ...[]byte) (int64, error) {
	args := make([]interface{}, len(members))
	for i, m := range members {
		args[i] = m
	}
	return c.r.Client.SAdd(ctx, key, args...).Result()
}

func (c *serverClient) SRem(ctx context.Context, key string, members ...[]byte) (int64, error) {
	args := make([]interface{}, len(members))
	for i, m := range members {
		args[i] = m
	}
	return c.r.Client.SRem(ctx, key, args...).Result()
}

func (c *serverClient) SMembers(ctx context.Context, key string) ([][]byte, error) {
	result, err := c.r.Client.SMembers(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	bytes := make([][]byte, len(result))
	for i, s := range result {
		bytes[i] = []byte(s)
	}
	return bytes, nil
}

func (c *serverClient) SIsMember(ctx context.Context, key string, member []byte) (bool, error) {
	return c.r.Client.SIsMember(ctx, key, member).Result()
}

func (c *serverClient) SCard(ctx context.Context, key string) (int64, error) {
	return c.r.Client.SCard(ctx, key).Result()
}

func (c *serverClient) ZAdd(ctx context.Context, key string, members ...ZMemberScore) (int64, error) {
	// Convert to go-redis Z type
	zMembers := make([]redis.Z, len(members))
	for i, m := range members {
		zMembers[i] = redis.Z{
			Score:  m.Score,
			Member: m.Member,
		}
	}
	return c.r.Client.ZAdd(ctx, key, zMembers...).Result()
}

func (c *serverClient) ZScore(ctx context.Context, key string, member []byte) (float64, bool, error) {
	score, err := c.r.Client.ZScore(ctx, key, string(member)).Result()
	if err != nil {
		// go-redis returns redis.Nil for missing member
		return 0, false, nil
	}
	return score, true, nil
}

func (c *serverClient) ZCard(ctx context.Context, key string) (int64, error) {
	return c.r.Client.ZCard(ctx, key).Result()
}

func (c *serverClient) DBSize(ctx context.Context) (int64, error) {
	return c.r.Client.DBSize(ctx).Result()
}

func (c *serverClient) FlushDB(ctx context.Context) error {
	return c.r.Client.FlushDB(ctx).Err()
}

func (c *serverClient) Do(ctx context.Context, args ...interface{}) (interface{}, error) {
	return c.r.Client.Do(ctx, args...).Result()
}
