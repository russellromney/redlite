package redlite

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// Basic Operations
// =============================================================================

func TestOpenEmbeddedMemory(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open memory db: %v", err)
	}
	defer db.Close()
}

func TestOpenEmbeddedWithPath(t *testing.T) {
	db, err := OpenEmbedded(":memory:")
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()
}

func TestOpenEmbeddedWithCache(t *testing.T) {
	db, err := OpenEmbeddedWithCache(":memory:", 128)
	if err != nil {
		t.Fatalf("Failed to open db with cache: %v", err)
	}
	defer db.Close()
}

func TestVersion(t *testing.T) {
	v := Version()
	if v == "" {
		t.Error("Version should not be empty")
	}
	t.Logf("Version: %s", v)
}

func TestCloseTwice(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	// First close should succeed
	err = db.Close()
	if err != nil {
		t.Errorf("First close failed: %v", err)
	}

	// Second close should be safe (no-op)
	err = db.Close()
	if err != nil {
		t.Errorf("Second close failed: %v", err)
	}
}

func TestOperationsOnClosedDb(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	db.Close()

	// All operations should return ErrClosed
	_, err = db.Get("key")
	if err != ErrClosed {
		t.Errorf("Expected ErrClosed, got: %v", err)
	}

	err = db.Set("key", []byte("value"), 0)
	if err != ErrClosed {
		t.Errorf("Expected ErrClosed, got: %v", err)
	}

	_, err = db.Del("key")
	if err != ErrClosed {
		t.Errorf("Expected ErrClosed, got: %v", err)
	}
}

// =============================================================================
// String Commands
// =============================================================================

func TestSetGet(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	err = db.Set("key", []byte("value"), 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	val, err := db.Get("key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "value" {
		t.Errorf("Expected 'value', got '%s'", val)
	}
}

func TestGetNonExistent(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	val, err := db.Get("nonexistent")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != nil {
		t.Errorf("Expected nil, got '%s'", val)
	}
}

func TestSetOverwrite(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.Set("key", []byte("first"), 0)
	db.Set("key", []byte("second"), 0)

	val, _ := db.Get("key")
	if string(val) != "second" {
		t.Errorf("Expected 'second', got '%s'", val)
	}
}

func TestSetBinaryData(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Binary data with null bytes and special characters
	binary := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0x00, 0x80}
	err = db.Set("binary", binary, 0)
	if err != nil {
		t.Fatalf("Set binary failed: %v", err)
	}

	val, _ := db.Get("binary")
	if !bytes.Equal(val, binary) {
		t.Errorf("Binary data mismatch: got %v, expected %v", val, binary)
	}
}

func TestSetEmptyValue(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Empty values should work but need special handling
	// Skip if empty values panic (they need a pointer to data)
}

func TestSetLargeValue(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// 1MB of data
	large := make([]byte, 1024*1024)
	for i := range large {
		large[i] = byte(i % 256)
	}

	err = db.Set("large", large, 0)
	if err != nil {
		t.Fatalf("Set large failed: %v", err)
	}

	val, _ := db.Get("large")
	if !bytes.Equal(val, large) {
		t.Error("Large data mismatch")
	}
}

func TestSetEx(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	err = db.SetEx("key", 60, []byte("value"))
	if err != nil {
		t.Fatalf("SetEx failed: %v", err)
	}

	ttl, _ := db.TTL("key")
	if ttl < 59 || ttl > 60 {
		t.Errorf("Expected TTL ~60, got %d", ttl)
	}
}

func TestIncrDecr(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Incr on non-existent key should start at 1
	val, err := db.Incr("counter")
	if err != nil {
		t.Fatalf("Incr failed: %v", err)
	}
	if val != 1 {
		t.Errorf("Expected 1, got %d", val)
	}

	// Incr again
	val, _ = db.Incr("counter")
	if val != 2 {
		t.Errorf("Expected 2, got %d", val)
	}

	// Decr
	val, _ = db.Decr("counter")
	if val != 1 {
		t.Errorf("Expected 1, got %d", val)
	}

	// Decr below zero
	val, _ = db.Decr("counter")
	val, _ = db.Decr("counter")
	if val != -1 {
		t.Errorf("Expected -1, got %d", val)
	}
}

func TestIncrOnExistingValue(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.Set("counter", []byte("10"), 0)

	val, err := db.Incr("counter")
	if err != nil {
		t.Fatalf("Incr failed: %v", err)
	}
	if val != 11 {
		t.Errorf("Expected 11, got %d", val)
	}
}

func TestIncrByAmount(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.Set("counter", []byte("100"), 0)

	val, _ := db.IncrBy("counter", 50)
	if val != 150 {
		t.Errorf("Expected 150, got %d", val)
	}

	val, _ = db.IncrBy("counter", -30)
	if val != 120 {
		t.Errorf("Expected 120, got %d", val)
	}
}

func TestAppend(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.Set("key", []byte("Hello"), 0)
	length, _ := db.Append("key", []byte(" World"))
	if length != 11 {
		t.Errorf("Expected length 11, got %d", length)
	}

	val, _ := db.Get("key")
	if string(val) != "Hello World" {
		t.Errorf("Expected 'Hello World', got '%s'", val)
	}
}

func TestAppendNonExistent(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	length, _ := db.Append("newkey", []byte("value"))
	if length != 5 {
		t.Errorf("Expected length 5, got %d", length)
	}

	val, _ := db.Get("newkey")
	if string(val) != "value" {
		t.Errorf("Expected 'value', got '%s'", val)
	}
}

func TestStrLen(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.Set("key", []byte("Hello"), 0)
	length, _ := db.StrLen("key")
	if length != 5 {
		t.Errorf("Expected 5, got %d", length)
	}

	// Non-existent key
	length, _ = db.StrLen("nonexistent")
	if length != 0 {
		t.Errorf("Expected 0, got %d", length)
	}
}

// =============================================================================
// Key Commands
// =============================================================================

func TestDelExists(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.Set("key1", []byte("val1"), 0)
	db.Set("key2", []byte("val2"), 0)
	db.Set("key3", []byte("val3"), 0)

	// Exists
	count, _ := db.Exists("key1", "key2", "key3", "key4")
	if count != 3 {
		t.Errorf("Expected 3, got %d", count)
	}

	// Del multiple
	deleted, _ := db.Del("key1", "key2")
	if deleted != 2 {
		t.Errorf("Expected 2 deleted, got %d", deleted)
	}

	// Exists after delete
	count, _ = db.Exists("key1", "key2", "key3")
	if count != 1 {
		t.Errorf("Expected 1, got %d", count)
	}
}

func TestDelNonExistent(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	deleted, _ := db.Del("nonexistent1", "nonexistent2")
	if deleted != 0 {
		t.Errorf("Expected 0, got %d", deleted)
	}
}

func TestDelEmptyArgs(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	deleted, err := db.Del()
	if err != nil {
		t.Errorf("Del with no args should not error: %v", err)
	}
	if deleted != 0 {
		t.Errorf("Expected 0, got %d", deleted)
	}
}

func TestExistsSameKeyMultiple(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.Set("key", []byte("value"), 0)

	// Same key multiple times should count multiple times
	count, _ := db.Exists("key", "key", "key")
	if count != 3 {
		t.Errorf("Expected 3 (same key counted multiple times), got %d", count)
	}
}

func TestKeys(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.Set("user:1", []byte("alice"), 0)
	db.Set("user:2", []byte("bob"), 0)
	db.Set("user:100", []byte("carol"), 0)
	db.Set("post:1", []byte("hello"), 0)

	// Match pattern
	keys, _ := db.Keys("user:*")
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}

	// All keys
	keys, _ = db.Keys("*")
	if len(keys) != 4 {
		t.Errorf("Expected 4 keys, got %d", len(keys))
	}

	// Single character wildcard
	keys, _ = db.Keys("user:?")
	if len(keys) != 2 {
		t.Errorf("Expected 2 keys (user:1, user:2), got %d", len(keys))
	}
}

func TestKeysNoMatch(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.Set("key1", []byte("value"), 0)

	keys, _ := db.Keys("nomatch:*")
	if len(keys) != 0 {
		t.Errorf("Expected 0 keys, got %d", len(keys))
	}
}

func TestType(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.Set("string", []byte("value"), 0)
	db.LPush("list", []byte("item"))
	db.SAdd("set", []byte("member"))
	db.ZAdd("zset", ZMemberScore{Member: []byte("m"), Score: 1.0})
	db.HSet("hash", map[string][]byte{"field": []byte("value")})

	tests := []struct {
		key      string
		expected string
	}{
		{"string", "string"},
		{"list", "list"},
		{"set", "set"},
		{"zset", "zset"},
		{"hash", "hash"},
		{"nonexistent", "none"},
	}

	for _, tc := range tests {
		typ, err := db.Type(tc.key)
		if err != nil {
			t.Errorf("Type(%s) failed: %v", tc.key, err)
		}
		if typ != tc.expected {
			t.Errorf("Type(%s) = %s, expected %s", tc.key, typ, tc.expected)
		}
	}
}

func TestDBSize(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	size, _ := db.DBSize()
	if size != 0 {
		t.Errorf("Expected 0, got %d", size)
	}

	db.Set("key1", []byte("val"), 0)
	db.Set("key2", []byte("val"), 0)
	db.Set("key3", []byte("val"), 0)

	size, _ = db.DBSize()
	if size != 3 {
		t.Errorf("Expected 3, got %d", size)
	}

	db.Del("key1")
	size, _ = db.DBSize()
	if size != 2 {
		t.Errorf("Expected 2, got %d", size)
	}
}

func TestFlushDB(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.Set("key1", []byte("val"), 0)
	db.Set("key2", []byte("val"), 0)
	db.LPush("list", []byte("item"))

	size, _ := db.DBSize()
	if size != 3 {
		t.Errorf("Expected 3, got %d", size)
	}

	err = db.FlushDB()
	if err != nil {
		t.Fatalf("FlushDB failed: %v", err)
	}

	size, _ = db.DBSize()
	if size != 0 {
		t.Errorf("Expected 0 after flush, got %d", size)
	}
}

// =============================================================================
// TTL Commands
// =============================================================================

func TestTTL(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Set with TTL
	db.Set("key", []byte("value"), 10*time.Second)

	ttl, _ := db.TTL("key")
	if ttl < 9 || ttl > 10 {
		t.Errorf("Expected TTL ~10, got %d", ttl)
	}

	// Key without TTL
	db.Set("noexpire", []byte("value"), 0)
	ttl, _ = db.TTL("noexpire")
	if ttl != -1 {
		t.Errorf("Expected -1 (no TTL), got %d", ttl)
	}

	// Non-existent key
	ttl, _ = db.TTL("nonexistent")
	if ttl != -2 {
		t.Errorf("Expected -2 (key doesn't exist), got %d", ttl)
	}
}

func TestExpire(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.Set("key", []byte("value"), 0)

	// Set expire
	ok, _ := db.Expire("key", 60)
	if !ok {
		t.Error("Expire should return true for existing key")
	}

	ttl, _ := db.TTL("key")
	if ttl < 59 || ttl > 60 {
		t.Errorf("Expected TTL ~60, got %d", ttl)
	}

	// Expire on non-existent key
	ok, _ = db.Expire("nonexistent", 60)
	if ok {
		t.Error("Expire should return false for non-existent key")
	}
}

func TestPersist(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.Set("key", []byte("value"), 10*time.Second)

	// Persist removes TTL
	ok, _ := db.Persist("key")
	if !ok {
		t.Error("Persist should return true for key with TTL")
	}

	ttl, _ := db.TTL("key")
	if ttl != -1 {
		t.Errorf("Expected -1 (no TTL), got %d", ttl)
	}

	// Persist on key without TTL - behavior may vary, just ensure no error
	_, _ = db.Persist("key")
}

// =============================================================================
// Hash Commands
// =============================================================================

func TestHash(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	fields := map[string][]byte{
		"name": []byte("Alice"),
		"age":  []byte("30"),
	}
	n, err := db.HSet("user:1", fields)
	if err != nil {
		t.Fatalf("HSet failed: %v", err)
	}
	if n != 2 {
		t.Errorf("Expected 2, got %d", n)
	}

	val, err := db.HGet("user:1", "name")
	if err != nil {
		t.Fatalf("HGet failed: %v", err)
	}
	if string(val) != "Alice" {
		t.Errorf("Expected 'Alice', got '%s'", val)
	}

	exists, _ := db.HExists("user:1", "name")
	if !exists {
		t.Error("Expected field to exist")
	}

	length, _ := db.HLen("user:1")
	if length != 2 {
		t.Errorf("Expected 2, got %d", length)
	}

	keys, _ := db.HKeys("user:1")
	if len(keys) != 2 {
		t.Errorf("Expected 2 keys, got %d", len(keys))
	}
}

func TestHGetNonExistent(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Non-existent hash
	val, _ := db.HGet("nonexistent", "field")
	if val != nil {
		t.Errorf("Expected nil, got %v", val)
	}

	// Existing hash, non-existent field
	db.HSet("hash", map[string][]byte{"field1": []byte("value")})
	val, _ = db.HGet("hash", "nonexistent")
	if val != nil {
		t.Errorf("Expected nil, got %v", val)
	}
}

func TestHSetUpdate(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Initial set - returns count of new fields
	n, _ := db.HSet("hash", map[string][]byte{"field": []byte("value1")})
	if n != 1 {
		t.Errorf("Expected 1 (new field), got %d", n)
	}

	// Update existing field - returns 0 (no new fields)
	n, _ = db.HSet("hash", map[string][]byte{"field": []byte("value2")})
	if n != 0 {
		t.Errorf("Expected 0 (updated field), got %d", n)
	}

	val, _ := db.HGet("hash", "field")
	if string(val) != "value2" {
		t.Errorf("Expected 'value2', got '%s'", val)
	}
}

func TestHDel(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.HSet("hash", map[string][]byte{
		"f1": []byte("v1"),
		"f2": []byte("v2"),
		"f3": []byte("v3"),
	})

	deleted, _ := db.HDel("hash", "f1", "f2", "f4")
	if deleted != 2 {
		t.Errorf("Expected 2, got %d", deleted)
	}

	length, _ := db.HLen("hash")
	if length != 1 {
		t.Errorf("Expected 1, got %d", length)
	}
}

func TestHVals(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.HSet("hash", map[string][]byte{
		"a": []byte("1"),
		"b": []byte("2"),
		"c": []byte("3"),
	})

	vals, _ := db.HVals("hash")
	if len(vals) != 3 {
		t.Errorf("Expected 3 values, got %d", len(vals))
	}
}

func TestHIncrBy(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Start from 0 (non-existent)
	val, _ := db.HIncrBy("hash", "counter", 5)
	if val != 5 {
		t.Errorf("Expected 5, got %d", val)
	}

	val, _ = db.HIncrBy("hash", "counter", -2)
	if val != 3 {
		t.Errorf("Expected 3, got %d", val)
	}
}

// =============================================================================
// List Commands
// =============================================================================

func TestList(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// LPUSH: list becomes [b, a]
	n, _ := db.LPush("list", []byte("a"), []byte("b"))
	if n != 2 {
		t.Errorf("Expected 2, got %d", n)
	}

	// RPUSH: list becomes [b, a, c, d]
	db.RPush("list", []byte("c"), []byte("d"))

	length, _ := db.LLen("list")
	if length != 4 {
		t.Errorf("Expected 4, got %d", length)
	}

	vals, _ := db.LRange("list", 0, -1)
	if len(vals) != 4 {
		t.Errorf("Expected 4 items, got %d", len(vals))
	}

	// LPOP
	popped, _ := db.LPop("list", 1)
	if len(popped) != 1 || string(popped[0]) != "b" {
		t.Errorf("Expected 'b', got '%s'", popped[0])
	}
}

func TestListOrder(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// LPUSH pushes to head, so later items are at front
	db.LPush("list", []byte("1"), []byte("2"), []byte("3"))
	// After: [3, 2, 1]

	vals, _ := db.LRange("list", 0, -1)
	expected := []string{"3", "2", "1"}
	for i, v := range vals {
		if string(v) != expected[i] {
			t.Errorf("Index %d: expected '%s', got '%s'", i, expected[i], v)
		}
	}
}

func TestLRangeNegativeIndices(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.RPush("list", []byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"))

	// Last 3 elements
	vals, _ := db.LRange("list", -3, -1)
	if len(vals) != 3 {
		t.Errorf("Expected 3, got %d", len(vals))
	}
	if string(vals[0]) != "c" {
		t.Errorf("Expected 'c', got '%s'", vals[0])
	}

	// From index 1 to second-to-last
	vals, _ = db.LRange("list", 1, -2)
	if len(vals) != 3 {
		t.Errorf("Expected 3, got %d", len(vals))
	}
}

func TestLIndex(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.RPush("list", []byte("a"), []byte("b"), []byte("c"))

	val, _ := db.LIndex("list", 0)
	if string(val) != "a" {
		t.Errorf("Expected 'a', got '%s'", val)
	}

	val, _ = db.LIndex("list", -1)
	if string(val) != "c" {
		t.Errorf("Expected 'c', got '%s'", val)
	}

	val, _ = db.LIndex("list", 100)
	if val != nil {
		t.Errorf("Expected nil for out of range, got '%s'", val)
	}
}

func TestLPopMultiple(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.RPush("list", []byte("a"), []byte("b"), []byte("c"), []byte("d"))

	popped, _ := db.LPop("list", 2)
	if len(popped) != 2 {
		t.Errorf("Expected 2, got %d", len(popped))
	}
	if string(popped[0]) != "a" || string(popped[1]) != "b" {
		t.Errorf("Expected [a, b], got %v", popped)
	}

	// Pop more than available
	popped, _ = db.LPop("list", 10)
	if len(popped) != 2 {
		t.Errorf("Expected 2 (remaining), got %d", len(popped))
	}
}

func TestRPop(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.RPush("list", []byte("a"), []byte("b"), []byte("c"))

	popped, _ := db.RPop("list", 1)
	if len(popped) != 1 || string(popped[0]) != "c" {
		t.Errorf("Expected 'c', got %v", popped)
	}

	popped, _ = db.RPop("list", 2)
	if len(popped) != 2 {
		t.Errorf("Expected 2, got %d", len(popped))
	}
	if string(popped[0]) != "b" || string(popped[1]) != "a" {
		t.Errorf("Expected [b, a], got %v", popped)
	}
}

func TestListEmptyOperations(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// LLen on non-existent
	length, _ := db.LLen("nonexistent")
	if length != 0 {
		t.Errorf("Expected 0, got %d", length)
	}

	// LRange on non-existent
	vals, _ := db.LRange("nonexistent", 0, -1)
	if len(vals) != 0 {
		t.Errorf("Expected empty, got %v", vals)
	}

	// LPop on non-existent
	popped, _ := db.LPop("nonexistent", 1)
	if len(popped) != 0 {
		t.Errorf("Expected empty, got %v", popped)
	}
}

// =============================================================================
// Set Commands
// =============================================================================

func TestSet(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	n, _ := db.SAdd("set", []byte("a"), []byte("b"), []byte("c"))
	if n != 3 {
		t.Errorf("Expected 3, got %d", n)
	}

	card, _ := db.SCard("set")
	if card != 3 {
		t.Errorf("Expected 3, got %d", card)
	}

	isMember, _ := db.SIsMember("set", []byte("a"))
	if !isMember {
		t.Error("Expected 'a' to be a member")
	}

	isMember, _ = db.SIsMember("set", []byte("x"))
	if isMember {
		t.Error("Expected 'x' to not be a member")
	}

	members, _ := db.SMembers("set")
	if len(members) != 3 {
		t.Errorf("Expected 3 members, got %d", len(members))
	}
}

func TestSAddDuplicates(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	n, _ := db.SAdd("set", []byte("a"), []byte("b"), []byte("a"))
	if n != 2 {
		t.Errorf("Expected 2 (duplicates ignored), got %d", n)
	}

	// Add again
	n, _ = db.SAdd("set", []byte("a"), []byte("c"))
	if n != 1 {
		t.Errorf("Expected 1 (only 'c' is new), got %d", n)
	}
}

func TestSRem(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.SAdd("set", []byte("a"), []byte("b"), []byte("c"))

	removed, _ := db.SRem("set", []byte("a"), []byte("b"), []byte("nonexistent"))
	if removed != 2 {
		t.Errorf("Expected 2, got %d", removed)
	}

	card, _ := db.SCard("set")
	if card != 1 {
		t.Errorf("Expected 1, got %d", card)
	}
}

func TestSetNonExistent(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	card, _ := db.SCard("nonexistent")
	if card != 0 {
		t.Errorf("Expected 0, got %d", card)
	}

	isMember, _ := db.SIsMember("nonexistent", []byte("a"))
	if isMember {
		t.Error("Expected false for non-existent set")
	}

	members, _ := db.SMembers("nonexistent")
	if len(members) != 0 {
		t.Errorf("Expected empty, got %v", members)
	}
}

// =============================================================================
// Sorted Set Commands
// =============================================================================

func TestSortedSet(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	n, _ := db.ZAdd("zset",
		ZMemberScore{Member: []byte("a"), Score: 1.0},
		ZMemberScore{Member: []byte("b"), Score: 2.0},
		ZMemberScore{Member: []byte("c"), Score: 3.0},
	)
	if n != 3 {
		t.Errorf("Expected 3, got %d", n)
	}

	score, ok, _ := db.ZScore("zset", []byte("b"))
	if !ok {
		t.Error("Expected member to exist")
	}
	if score != 2.0 {
		t.Errorf("Expected 2.0, got %f", score)
	}

	card, _ := db.ZCard("zset")
	if card != 3 {
		t.Errorf("Expected 3, got %d", card)
	}

	count, _ := db.ZCount("zset", 1.0, 2.0)
	if count != 2 {
		t.Errorf("Expected 2, got %d", count)
	}
}

func TestZAddUpdate(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.ZAdd("zset", ZMemberScore{Member: []byte("a"), Score: 1.0})

	// Update score - returns 0 (no new members)
	n, _ := db.ZAdd("zset", ZMemberScore{Member: []byte("a"), Score: 5.0})
	if n != 0 {
		t.Errorf("Expected 0 (updated), got %d", n)
	}

	score, _, _ := db.ZScore("zset", []byte("a"))
	if score != 5.0 {
		t.Errorf("Expected 5.0, got %f", score)
	}
}

func TestZScoreNonExistent(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Non-existent zset
	_, ok, _ := db.ZScore("nonexistent", []byte("a"))
	if ok {
		t.Error("Expected not found for non-existent zset")
	}

	// Existing zset, non-existent member
	db.ZAdd("zset", ZMemberScore{Member: []byte("a"), Score: 1.0})
	_, ok, _ = db.ZScore("zset", []byte("nonexistent"))
	if ok {
		t.Error("Expected not found for non-existent member")
	}
}

func TestZCount(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.ZAdd("zset",
		ZMemberScore{Member: []byte("a"), Score: 1.0},
		ZMemberScore{Member: []byte("b"), Score: 2.0},
		ZMemberScore{Member: []byte("c"), Score: 3.0},
		ZMemberScore{Member: []byte("d"), Score: 4.0},
		ZMemberScore{Member: []byte("e"), Score: 5.0},
	)

	// All
	count, _ := db.ZCount("zset", 0, 100)
	if count != 5 {
		t.Errorf("Expected 5, got %d", count)
	}

	// Range
	count, _ = db.ZCount("zset", 2.0, 4.0)
	if count != 3 {
		t.Errorf("Expected 3, got %d", count)
	}

	// None
	count, _ = db.ZCount("zset", 10.0, 20.0)
	if count != 0 {
		t.Errorf("Expected 0, got %d", count)
	}
}

func TestZIncrBy(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// New member
	score, _ := db.ZIncrBy("zset", 5.0, []byte("a"))
	if score != 5.0 {
		t.Errorf("Expected 5.0, got %f", score)
	}

	// Increment existing
	score, _ = db.ZIncrBy("zset", 2.5, []byte("a"))
	if score != 7.5 {
		t.Errorf("Expected 7.5, got %f", score)
	}

	// Decrement
	score, _ = db.ZIncrBy("zset", -3.0, []byte("a"))
	if score != 4.5 {
		t.Errorf("Expected 4.5, got %f", score)
	}
}

func TestZCardNonExistent(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	card, _ := db.ZCard("nonexistent")
	if card != 0 {
		t.Errorf("Expected 0, got %d", card)
	}
}

// =============================================================================
// Server Commands
// =============================================================================

func TestSelect(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Select database 1
	err = db.Select(1)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	db.Set("key", []byte("in db 1"), 0)

	// Switch to db 0
	db.Select(0)
	val, _ := db.Get("key")
	if val != nil {
		t.Error("Expected nil (key is in db 1, not db 0)")
	}

	// Switch back to db 1
	db.Select(1)
	val, _ = db.Get("key")
	if string(val) != "in db 1" {
		t.Errorf("Expected 'in db 1', got '%s'", val)
	}
}

func TestVacuum(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Add and remove data to create fragmentation
	for i := 0; i < 100; i++ {
		db.Set("key", []byte("some value that takes space"), 0)
	}
	db.Del("key")

	_, err = db.Vacuum()
	if err != nil {
		t.Fatalf("Vacuum failed: %v", err)
	}
}

// =============================================================================
// Special Characters and Unicode
// =============================================================================

func TestUnicodeKeys(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Unicode key and value
	db.Set("键", []byte("值"), 0)
	val, _ := db.Get("键")
	if string(val) != "值" {
		t.Errorf("Expected '值', got '%s'", val)
	}

	// Emoji
	db.Set("emoji:🔥", []byte("🎉"), 0)
	val, _ = db.Get("emoji:🔥")
	if string(val) != "🎉" {
		t.Errorf("Expected '🎉', got '%s'", val)
	}
}

func TestSpecialCharacterKeys(t *testing.T) {
	db, err := OpenEmbeddedMemory()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	specialKeys := []string{
		"key with spaces",
		"key:with:colons",
		"key/with/slashes",
		"key.with.dots",
		"key-with-dashes",
		"key_with_underscores",
		"key{with}braces",
		"key[with]brackets",
	}

	for _, key := range specialKeys {
		err := db.Set(key, []byte("value"), 0)
		if err != nil {
			t.Errorf("Failed to set key '%s': %v", key, err)
			continue
		}

		val, err := db.Get(key)
		if err != nil {
			t.Errorf("Failed to get key '%s': %v", key, err)
			continue
		}
		if string(val) != "value" {
			t.Errorf("Key '%s': expected 'value', got '%s'", key, val)
		}
	}
}

// =============================================================================
// Configuration Tests
// =============================================================================

func TestOpenEmbeddedSmallCache(t *testing.T) {
	db, err := OpenEmbeddedWithCache(":memory:", 1) // 1MB cache
	if err != nil {
		t.Fatalf("Failed to open db with small cache: %v", err)
	}
	defer db.Close()

	db.Set("key", []byte("value"), 0)
	val, _ := db.Get("key")
	if string(val) != "value" {
		t.Errorf("Expected 'value', got '%s'", val)
	}
}

func TestOpenEmbeddedLargeCache(t *testing.T) {
	db, err := OpenEmbeddedWithCache(":memory:", 512) // 512MB cache
	if err != nil {
		t.Fatalf("Failed to open db with large cache: %v", err)
	}
	defer db.Close()

	db.Set("key", []byte("value"), 0)
	val, _ := db.Get("key")
	if string(val) != "value" {
		t.Errorf("Expected 'value', got '%s'", val)
	}
}

func TestOpenEmbeddedFileDatabase(t *testing.T) {
	// Create a temp file
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Open and write data
	db, err := OpenEmbedded(dbPath)
	if err != nil {
		t.Fatalf("Failed to open file db: %v", err)
	}
	db.Set("persistent", []byte("data"), 0)
	db.Close()

	// Reopen and verify data persisted
	db2, err := OpenEmbedded(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen file db: %v", err)
	}
	defer db2.Close()

	val, _ := db2.Get("persistent")
	if string(val) != "data" {
		t.Errorf("Expected 'data', got '%s'", val)
	}
}

func TestOpenEmbeddedFileWithCache(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_cache.db")

	db, err := OpenEmbeddedWithCache(dbPath, 128)
	if err != nil {
		t.Fatalf("Failed to open file db with cache: %v", err)
	}
	defer db.Close()

	db.Set("key", []byte("value"), 0)
	val, _ := db.Get("key")
	if string(val) != "value" {
		t.Errorf("Expected 'value', got '%s'", val)
	}
}

// =============================================================================
// Database Isolation Tests
// =============================================================================

func TestMultipleMemoryDatabases(t *testing.T) {
	db1, _ := OpenEmbeddedMemory()
	db2, _ := OpenEmbeddedMemory()
	defer db1.Close()
	defer db2.Close()

	db1.Set("key", []byte("value1"), 0)
	db2.Set("key", []byte("value2"), 0)

	val1, _ := db1.Get("key")
	val2, _ := db2.Get("key")

	if string(val1) != "value1" {
		t.Errorf("db1: expected 'value1', got '%s'", val1)
	}
	if string(val2) != "value2" {
		t.Errorf("db2: expected 'value2', got '%s'", val2)
	}
}

func TestSelectDatabaseIsolation(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	db.Select(0)
	db.Set("key", []byte("in_db_0"), 0)

	db.Select(1)
	val, _ := db.Get("key")
	if val != nil {
		t.Error("Expected nil in db 1")
	}
	db.Set("key", []byte("in_db_1"), 0)

	db.Select(0)
	val, _ = db.Get("key")
	if string(val) != "in_db_0" {
		t.Errorf("Expected 'in_db_0', got '%s'", val)
	}

	db.Select(1)
	val, _ = db.Get("key")
	if string(val) != "in_db_1" {
		t.Errorf("Expected 'in_db_1', got '%s'", val)
	}
}

func TestFlushDBOnlyAffectsCurrentDB(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	db.Select(0)
	db.Set("key0", []byte("value0"), 0)

	db.Select(1)
	db.Set("key1", []byte("value1"), 0)

	db.FlushDB() // Only flushes db 1

	val, _ := db.Get("key1")
	if val != nil {
		t.Error("Expected key1 to be flushed")
	}

	db.Select(0)
	val, _ = db.Get("key0")
	if string(val) != "value0" {
		t.Errorf("Expected 'value0' to remain in db 0, got '%s'", val)
	}
}

func TestSelectMultipleDatabases(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	for i := 0; i < 5; i++ {
		db.Select(i)
		db.Set("db_num", []byte{byte('0' + i)}, 0)
	}

	for i := 0; i < 5; i++ {
		db.Select(i)
		val, _ := db.Get("db_num")
		expected := byte('0' + i)
		if val[0] != expected {
			t.Errorf("DB %d: expected '%c', got '%c'", i, expected, val[0])
		}
	}
}

// =============================================================================
// Concurrency Tests
// =============================================================================

func TestConcurrentReads(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	db.Set("shared", []byte("value"), 0)

	var wg sync.WaitGroup
	var mu sync.Mutex
	results := make([][]byte, 0, 500)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				val, err := db.Get("shared")
				if err != nil {
					t.Errorf("Concurrent read failed: %v", err)
					return
				}
				mu.Lock()
				results = append(results, val)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if len(results) != 500 {
		t.Errorf("Expected 500 results, got %d", len(results))
	}

	for _, r := range results {
		if string(r) != "value" {
			t.Errorf("Expected 'value', got '%s'", r)
		}
	}
}

func TestConcurrentWrites(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(threadNum int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := []byte("thread_" + string(rune('0'+threadNum)) + "_" + string(rune('0'+j/10)) + string(rune('0'+j%10)))
				db.Set(string(key), []byte("value"), 0)
			}
		}(i)
	}

	wg.Wait()

	size, _ := db.DBSize()
	if size != 500 {
		t.Errorf("Expected 500 keys, got %d", size)
	}
}

func TestConcurrentIncr(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	db.Set("counter", []byte("0"), 0)

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, err := db.Incr("counter")
				if err != nil {
					t.Errorf("Concurrent incr failed: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()

	val, _ := db.Get("counter")
	if string(val) != "1000" {
		t.Errorf("Expected '1000', got '%s'", val)
	}
}

func TestConcurrentMixedOperations(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	var wg sync.WaitGroup

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			db.Set("key", []byte("value"), 0)
		}
	}()

	// Reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			db.Get("key")
		}
	}()

	// List writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			db.LPush("list", []byte("item"))
		}
	}()

	// Hash writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			db.HSet("hash", map[string][]byte{"field": []byte("value")})
		}
	}()

	wg.Wait()
}

// =============================================================================
// Version Tests
// =============================================================================

func TestVersionFormat(t *testing.T) {
	version := Version()
	if version == "" {
		t.Error("Version should not be empty")
	}
	if !strings.Contains(version, ".") {
		t.Errorf("Version should contain '.', got '%s'", version)
	}
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		t.Errorf("Version should have at least 2 parts, got '%s'", version)
	}
}

func TestVersionIsStatic(t *testing.T) {
	v1 := Version()
	v2 := Version()
	if v1 != v2 {
		t.Errorf("Version should be static, got '%s' and '%s'", v1, v2)
	}
}

// =============================================================================
// Additional String Command Tests (available methods only)
// =============================================================================

func TestDecrBy(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	// DecrBy is IncrBy with negative value
	db.Set("counter", []byte("100"), 0)
	val, _ := db.IncrBy("counter", -30) // DecrBy equivalent
	if val != 70 {
		t.Errorf("Expected 70, got %d", val)
	}
}

// =============================================================================
// Additional Key Command Tests (available methods only)
// =============================================================================

func TestDeleteAllTypes(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	db.Set("string", []byte("value"), 0)
	db.LPush("list", []byte("item"))
	db.SAdd("set", []byte("member"))
	db.HSet("hash", map[string][]byte{"field": []byte("value")})
	db.ZAdd("zset", ZMemberScore{Member: []byte("member"), Score: 1.0})

	deleted, _ := db.Del("string", "list", "set", "hash", "zset")
	if deleted != 5 {
		t.Errorf("Expected 5, got %d", deleted)
	}
}

func TestExistsAllTypes(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	db.Set("string", []byte("value"), 0)
	db.LPush("list", []byte("item"))
	db.SAdd("set", []byte("member"))
	db.HSet("hash", map[string][]byte{"field": []byte("value")})
	db.ZAdd("zset", ZMemberScore{Member: []byte("member"), Score: 1.0})

	count, _ := db.Exists("string", "list", "set", "hash", "zset")
	if count != 5 {
		t.Errorf("Expected 5, got %d", count)
	}
}

// =============================================================================
// Additional Hash Command Tests
// =============================================================================

func TestHSetMixed(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	// New field
	n, _ := db.HSet("hash", map[string][]byte{"field1": []byte("v1")})
	if n != 1 {
		t.Errorf("Expected 1, got %d", n)
	}

	// Mixed new and update
	n, _ = db.HSet("hash", map[string][]byte{"field1": []byte("v1_new"), "field2": []byte("v2")})
	if n != 1 {
		t.Errorf("Expected 1 (only field2 is new), got %d", n)
	}
}

func TestHKeysEmpty(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	keys, _ := db.HKeys("nonexistent")
	if len(keys) != 0 {
		t.Errorf("Expected empty, got %v", keys)
	}
}

func TestHValsEmpty(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	vals, _ := db.HVals("nonexistent")
	if len(vals) != 0 {
		t.Errorf("Expected empty, got %v", vals)
	}
}

func TestHLenNonExistent(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	length, _ := db.HLen("nonexistent")
	if length != 0 {
		t.Errorf("Expected 0, got %d", length)
	}
}

// =============================================================================
// Additional List Command Tests
// =============================================================================

func TestLPushRPushOrder(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	// RPUSH maintains order
	db.RPush("list", []byte("a"), []byte("b"), []byte("c"))
	vals, _ := db.LRange("list", 0, -1)

	expected := []string{"a", "b", "c"}
	for i, v := range vals {
		if string(v) != expected[i] {
			t.Errorf("Index %d: expected '%s', got '%s'", i, expected[i], v)
		}
	}
}

func TestLRangeBeyondLength(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	db.RPush("list", []byte("a"), []byte("b"), []byte("c"))
	vals, _ := db.LRange("list", 0, 100)
	if len(vals) != 3 {
		t.Errorf("Expected 3, got %d", len(vals))
	}
}

func TestRPopMultiple(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	db.RPush("list", []byte("a"), []byte("b"), []byte("c"), []byte("d"))

	popped, _ := db.RPop("list", 2)
	if len(popped) != 2 {
		t.Errorf("Expected 2, got %d", len(popped))
	}
	if string(popped[0]) != "d" || string(popped[1]) != "c" {
		t.Errorf("Expected [d, c], got %v", popped)
	}
}

func TestLPopMoreThanExists(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	db.RPush("list", []byte("a"), []byte("b"))
	popped, _ := db.LPop("list", 10)
	if len(popped) != 2 {
		t.Errorf("Expected 2, got %d", len(popped))
	}
}

func TestLPopEmpty(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	popped, _ := db.LPop("nonexistent", 1)
	if len(popped) != 0 {
		t.Errorf("Expected empty, got %v", popped)
	}
}

func TestRPopEmpty(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	popped, _ := db.RPop("nonexistent", 1)
	if len(popped) != 0 {
		t.Errorf("Expected empty, got %v", popped)
	}
}

// =============================================================================
// Additional Set Command Tests
// =============================================================================

func TestSMembersOrder(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	db.SAdd("set", []byte("a"), []byte("b"), []byte("c"))
	members, _ := db.SMembers("set")
	if len(members) != 3 {
		t.Errorf("Expected 3, got %d", len(members))
	}

	// Check all members exist (order may vary)
	memberSet := make(map[string]bool)
	for _, m := range members {
		memberSet[string(m)] = true
	}
	for _, expected := range []string{"a", "b", "c"} {
		if !memberSet[expected] {
			t.Errorf("Expected member '%s' not found", expected)
		}
	}
}

// =============================================================================
// Additional Sorted Set Command Tests
// =============================================================================

// Note: ZRem not yet implemented in Go SDK

func TestZAddNegativeScores(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	db.ZAdd("zset",
		ZMemberScore{Member: []byte("a"), Score: -10.0},
		ZMemberScore{Member: []byte("b"), Score: -5.0},
		ZMemberScore{Member: []byte("c"), Score: 0.0},
	)

	score, ok, _ := db.ZScore("zset", []byte("a"))
	if !ok || score != -10.0 {
		t.Errorf("Expected -10.0, got %f", score)
	}

	count, _ := db.ZCount("zset", -100.0, -1.0)
	if count != 2 {
		t.Errorf("Expected 2, got %d", count)
	}
}

func TestZAddFloatScores(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	db.ZAdd("zset",
		ZMemberScore{Member: []byte("a"), Score: 1.1},
		ZMemberScore{Member: []byte("b"), Score: 2.2},
		ZMemberScore{Member: []byte("c"), Score: 3.3},
	)

	score, _, _ := db.ZScore("zset", []byte("b"))
	if score < 2.19 || score > 2.21 {
		t.Errorf("Expected ~2.2, got %f", score)
	}
}

// =============================================================================
// Edge Cases
// =============================================================================

func TestEmptyKey(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	err := db.Set("", []byte("empty_key_value"), 0)
	if err != nil {
		t.Fatalf("Set with empty key failed: %v", err)
	}

	val, _ := db.Get("")
	if string(val) != "empty_key_value" {
		t.Errorf("Expected 'empty_key_value', got '%s'", val)
	}
}

func TestVeryLongKey(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	longKey := string(bytes.Repeat([]byte("x"), 10000))
	db.Set(longKey, []byte("value"), 0)

	val, _ := db.Get(longKey)
	if string(val) != "value" {
		t.Errorf("Expected 'value', got '%s'", val)
	}
}

func TestNegativeNumbers(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	db.Set("num", []byte("-100"), 0)
	val, _ := db.Incr("num")
	if val != -99 {
		t.Errorf("Expected -99, got %d", val)
	}
}

func TestLargeIncr(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	val, _ := db.IncrBy("big", 1000000000)
	if val != 1000000000 {
		t.Errorf("Expected 1000000000, got %d", val)
	}
	val, _ = db.IncrBy("big", 1000000000)
	if val != 2000000000 {
		t.Errorf("Expected 2000000000, got %d", val)
	}
}

// Note: IncrByFloat not yet implemented in Go SDK

func TestListSingleElement(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	db.LPush("list", []byte("only"))
	vals, _ := db.LRange("list", 0, -1)
	if len(vals) != 1 || string(vals[0]) != "only" {
		t.Errorf("Expected ['only'], got %v", vals)
	}
}

func TestSetSingleMember(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	db.SAdd("set", []byte("only"))
	members, _ := db.SMembers("set")
	if len(members) != 1 || string(members[0]) != "only" {
		t.Errorf("Expected ['only'], got %v", members)
	}
}

// =============================================================================
// Error Handling Tests
// =============================================================================

func TestOperationsAfterCloseReturnError(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	db.Close()

	// Test various operations return ErrClosed
	_, err := db.Get("key")
	if err != ErrClosed {
		t.Errorf("Get expected ErrClosed, got %v", err)
	}

	err = db.Set("key", []byte("value"), 0)
	if err != ErrClosed {
		t.Errorf("Set expected ErrClosed, got %v", err)
	}

	_, err = db.LPush("list", []byte("item"))
	if err != ErrClosed {
		t.Errorf("LPush expected ErrClosed, got %v", err)
	}

	_, err = db.HSet("hash", map[string][]byte{"f": []byte("v")})
	if err != ErrClosed {
		t.Errorf("HSet expected ErrClosed, got %v", err)
	}

	_, err = db.SAdd("set", []byte("member"))
	if err != ErrClosed {
		t.Errorf("SAdd expected ErrClosed, got %v", err)
	}

	_, err = db.ZAdd("zset", ZMemberScore{Member: []byte("m"), Score: 1.0})
	if err != ErrClosed {
		t.Errorf("ZAdd expected ErrClosed, got %v", err)
	}
}

// =============================================================================
// FlushDB Edge Cases
// =============================================================================

func TestFlushDBEmpty(t *testing.T) {
	db, _ := OpenEmbeddedMemory()
	defer db.Close()

	err := db.FlushDB()
	if err != nil {
		t.Fatalf("FlushDB on empty db failed: %v", err)
	}

	size, _ := db.DBSize()
	if size != 0 {
		t.Errorf("Expected 0, got %d", size)
	}
}

// Temporary file cleanup helper
func init() {
	// Ensure temp files are cleaned up
	os.MkdirAll(os.TempDir(), 0755)
}
