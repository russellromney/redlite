package redlite

/*
#cgo LDFLAGS: -L${SRCDIR}/../../crates/redlite-ffi/target/release -lredlite_ffi
#cgo darwin LDFLAGS: -Wl,-rpath,${SRCDIR}/../../crates/redlite-ffi/target/release
#cgo linux LDFLAGS: -Wl,-rpath,${SRCDIR}/../../crates/redlite-ffi/target/release

#include <stdlib.h>
#include "redlite.h"
*/
import "C"
import (
	"errors"
	"math"
	"time"
	"unsafe"
)

// EmbeddedDb provides a direct embedded connection to redlite via FFI.
// No server process, no network overhead - true embedded operation.
type EmbeddedDb struct {
	handle *C.RedliteDb
}

// ErrClosed is returned when operations are attempted on a closed database.
var ErrClosed = errors.New("database is closed")

// OpenEmbedded opens an embedded database at the given path.
// Use ":memory:" for an in-memory database.
func OpenEmbedded(path string) (*EmbeddedDb, error) {
	return OpenEmbeddedWithCache(path, 64)
}

// OpenEmbeddedMemory opens an in-memory embedded database.
func OpenEmbeddedMemory() (*EmbeddedDb, error) {
	handle := C.redlite_open_memory()
	if handle == nil {
		return nil, getLastError()
	}
	return &EmbeddedDb{handle: handle}, nil
}

// OpenEmbeddedWithCache opens an embedded database with custom cache size.
func OpenEmbeddedWithCache(path string, cacheMB int64) (*EmbeddedDb, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	handle := C.redlite_open_with_cache(cPath, C.int64_t(cacheMB))
	if handle == nil {
		return nil, getLastError()
	}
	return &EmbeddedDb{handle: handle}, nil
}

// Close closes the database and releases resources.
func (db *EmbeddedDb) Close() error {
	if db.handle != nil {
		C.redlite_close(db.handle)
		db.handle = nil
	}
	return nil
}

func (db *EmbeddedDb) checkOpen() error {
	if db.handle == nil {
		return ErrClosed
	}
	return nil
}

func getLastError() error {
	errStr := C.redlite_last_error()
	if errStr == nil {
		return errors.New("unknown error")
	}
	defer C.redlite_free_string(errStr)
	return errors.New(C.GoString(errStr))
}

func bytesToGo(rb C.RedliteBytes) []byte {
	if rb.data == nil || rb.len == 0 {
		return nil
	}
	result := C.GoBytes(unsafe.Pointer(rb.data), C.int(rb.len))
	C.redlite_free_bytes(rb)
	return result
}

func stringArrayToGo(arr C.RedliteStringArray) []string {
	if arr.strings == nil || arr.len == 0 {
		return nil
	}
	defer C.redlite_free_string_array(arr)

	result := make([]string, arr.len)
	// Create a slice from the C array
	cStrings := (*[1 << 30]*C.char)(unsafe.Pointer(arr.strings))[:arr.len:arr.len]
	for i, s := range cStrings {
		result[i] = C.GoString(s)
	}
	return result
}

func bytesArrayToGo(arr C.RedliteBytesArray) [][]byte {
	if arr.items == nil || arr.len == 0 {
		return nil
	}
	defer C.redlite_free_bytes_array(arr)

	result := make([][]byte, arr.len)
	items := (*[1 << 30]C.RedliteBytes)(unsafe.Pointer(arr.items))[:arr.len:arr.len]
	for i, item := range items {
		if item.data != nil && item.len > 0 {
			result[i] = C.GoBytes(unsafe.Pointer(item.data), C.int(item.len))
		}
	}
	return result
}

// =============================================================================
// String Commands
// =============================================================================

// Get returns the value of a key.
func (db *EmbeddedDb) Get(key string) ([]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_get(db.handle, cKey)
	return bytesToGo(result), nil
}

// Set sets the value of a key with optional TTL.
func (db *EmbeddedDb) Set(key string, value []byte, ttl time.Duration) error {
	if err := db.checkOpen(); err != nil {
		return err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	ttlSeconds := int64(0)
	if ttl > 0 {
		ttlSeconds = int64(ttl.Seconds())
	}

	var dataPtr *C.uint8_t
	if len(value) > 0 {
		dataPtr = (*C.uint8_t)(unsafe.Pointer(&value[0]))
	}

	result := C.redlite_set(db.handle, cKey, dataPtr, C.size_t(len(value)), C.int64_t(ttlSeconds))
	if result < 0 {
		return getLastError()
	}
	return nil
}

// SetEx sets the value with expiration in seconds.
func (db *EmbeddedDb) SetEx(key string, seconds int64, value []byte) error {
	if err := db.checkOpen(); err != nil {
		return err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	var dataPtr *C.uint8_t
	if len(value) > 0 {
		dataPtr = (*C.uint8_t)(unsafe.Pointer(&value[0]))
	}

	result := C.redlite_setex(db.handle, cKey, C.int64_t(seconds), dataPtr, C.size_t(len(value)))
	if result < 0 {
		return getLastError()
	}
	return nil
}

// Incr increments a key by 1.
func (db *EmbeddedDb) Incr(key string) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_incr(db.handle, cKey)
	if result == C.int64_t(math.MinInt64) {
		return 0, getLastError()
	}
	return int64(result), nil
}

// Decr decrements a key by 1.
func (db *EmbeddedDb) Decr(key string) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_decr(db.handle, cKey)
	if result == C.int64_t(math.MinInt64) {
		return 0, getLastError()
	}
	return int64(result), nil
}

// IncrBy increments a key by amount.
func (db *EmbeddedDb) IncrBy(key string, amount int64) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_incrby(db.handle, cKey, C.int64_t(amount))
	if result == C.int64_t(math.MinInt64) {
		return 0, getLastError()
	}
	return int64(result), nil
}

// Append appends value to key.
func (db *EmbeddedDb) Append(key string, value []byte) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	var dataPtr *C.uint8_t
	if len(value) > 0 {
		dataPtr = (*C.uint8_t)(unsafe.Pointer(&value[0]))
	}

	result := C.redlite_append(db.handle, cKey, dataPtr, C.size_t(len(value)))
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// StrLen returns the length of the value stored at key.
func (db *EmbeddedDb) StrLen(key string) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_strlen(db.handle, cKey)
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// GetDel returns and deletes the value of a key.
func (db *EmbeddedDb) GetDel(key string) ([]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_getdel(db.handle, cKey)
	return bytesToGo(result), nil
}

// GetRange returns a substring of the value stored at key.
func (db *EmbeddedDb) GetRange(key string, start, end int64) ([]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_getrange(db.handle, cKey, C.int64_t(start), C.int64_t(end))
	return bytesToGo(result), nil
}

// SetRange overwrites part of the value stored at key.
func (db *EmbeddedDb) SetRange(key string, offset int64, value []byte) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	var dataPtr *C.uint8_t
	if len(value) > 0 {
		dataPtr = (*C.uint8_t)(unsafe.Pointer(&value[0]))
	}

	result := C.redlite_setrange(db.handle, cKey, C.int64_t(offset), dataPtr, C.size_t(len(value)))
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// DecrBy decrements a key by amount.
func (db *EmbeddedDb) DecrBy(key string, amount int64) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_decrby(db.handle, cKey, C.int64_t(amount))
	if result == C.int64_t(math.MinInt64) {
		return 0, getLastError()
	}
	return int64(result), nil
}

// IncrByFloat increments a key by a float amount.
func (db *EmbeddedDb) IncrByFloat(key string, amount float64) (string, error) {
	if err := db.checkOpen(); err != nil {
		return "", err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_incrbyfloat(db.handle, cKey, C.double(amount))
	if result == nil {
		return "", getLastError()
	}
	defer C.redlite_free_string(result)
	return C.GoString(result), nil
}

// PSetEx sets the value with expiration in milliseconds.
func (db *EmbeddedDb) PSetEx(key string, milliseconds int64, value []byte) error {
	if err := db.checkOpen(); err != nil {
		return err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	var dataPtr *C.uint8_t
	if len(value) > 0 {
		dataPtr = (*C.uint8_t)(unsafe.Pointer(&value[0]))
	}

	result := C.redlite_psetex(db.handle, cKey, C.int64_t(milliseconds), dataPtr, C.size_t(len(value)))
	if result < 0 {
		return getLastError()
	}
	return nil
}

// MGet returns the values of multiple keys.
func (db *EmbeddedDb) MGet(keys ...string) ([][]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}

	cKeys := make([]*C.char, len(keys))
	for i, k := range keys {
		cKeys[i] = C.CString(k)
		defer C.free(unsafe.Pointer(cKeys[i]))
	}

	result := C.redlite_mget(db.handle, &cKeys[0], C.size_t(len(keys)))
	return bytesArrayToGo(result), nil
}

// MSet sets multiple key-value pairs.
func (db *EmbeddedDb) MSet(pairs map[string][]byte) error {
	if err := db.checkOpen(); err != nil {
		return err
	}
	if len(pairs) == 0 {
		return nil
	}

	// Allocate C array for key-value pairs
	pairsPtr := C.malloc(C.size_t(len(pairs)) * C.size_t(unsafe.Sizeof(C.RedliteKV{})))
	defer C.free(pairsPtr)
	cPairs := (*[1 << 30]C.RedliteKV)(pairsPtr)[:len(pairs):len(pairs)]

	// Track allocations for cleanup
	var cKeys []*C.char
	var cDatas []unsafe.Pointer
	defer func() {
		for _, k := range cKeys {
			C.free(unsafe.Pointer(k))
		}
		for _, d := range cDatas {
			C.free(d)
		}
	}()

	i := 0
	for key, value := range pairs {
		cKey := C.CString(key)
		cKeys = append(cKeys, cKey)

		if len(value) > 0 {
			cData := C.CBytes(value)
			cDatas = append(cDatas, cData)
			cPairs[i] = C.RedliteKV{
				key:       cKey,
				value:     (*C.uint8_t)(cData),
				value_len: C.size_t(len(value)),
			}
		} else {
			cPairs[i] = C.RedliteKV{
				key:       cKey,
				value:     nil,
				value_len: 0,
			}
		}
		i++
	}

	result := C.redlite_mset(db.handle, (*C.RedliteKV)(pairsPtr), C.size_t(len(pairs)))
	if result < 0 {
		return getLastError()
	}
	return nil
}

// =============================================================================
// Key Commands
// =============================================================================

// Del deletes one or more keys.
func (db *EmbeddedDb) Del(keys ...string) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}
	if len(keys) == 0 {
		return 0, nil
	}

	cKeys := make([]*C.char, len(keys))
	for i, k := range keys {
		cKeys[i] = C.CString(k)
		defer C.free(unsafe.Pointer(cKeys[i]))
	}

	result := C.redlite_del(db.handle, &cKeys[0], C.size_t(len(keys)))
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// Exists checks if keys exist.
func (db *EmbeddedDb) Exists(keys ...string) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}
	if len(keys) == 0 {
		return 0, nil
	}

	cKeys := make([]*C.char, len(keys))
	for i, k := range keys {
		cKeys[i] = C.CString(k)
		defer C.free(unsafe.Pointer(cKeys[i]))
	}

	result := C.redlite_exists(db.handle, &cKeys[0], C.size_t(len(keys)))
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// Type returns the type of a key.
func (db *EmbeddedDb) Type(key string) (string, error) {
	if err := db.checkOpen(); err != nil {
		return "", err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_type(db.handle, cKey)
	if result == nil {
		return "none", nil
	}
	defer C.redlite_free_string(result)
	return C.GoString(result), nil
}

// TTL returns the TTL of a key in seconds.
func (db *EmbeddedDb) TTL(key string) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_ttl(db.handle, cKey)
	return int64(result), nil
}

// Expire sets a TTL on a key.
func (db *EmbeddedDb) Expire(key string, seconds int64) (bool, error) {
	if err := db.checkOpen(); err != nil {
		return false, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_expire(db.handle, cKey, C.int64_t(seconds))
	if result < 0 {
		return false, getLastError()
	}
	return result == 1, nil
}

// Persist removes the TTL from a key.
func (db *EmbeddedDb) Persist(key string) (bool, error) {
	if err := db.checkOpen(); err != nil {
		return false, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_persist(db.handle, cKey)
	if result < 0 {
		return false, getLastError()
	}
	return result == 1, nil
}

// PTTL returns the TTL of a key in milliseconds.
func (db *EmbeddedDb) PTTL(key string) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_pttl(db.handle, cKey)
	return int64(result), nil
}

// PExpire sets a TTL on a key in milliseconds.
func (db *EmbeddedDb) PExpire(key string, milliseconds int64) (bool, error) {
	if err := db.checkOpen(); err != nil {
		return false, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_pexpire(db.handle, cKey, C.int64_t(milliseconds))
	if result < 0 {
		return false, getLastError()
	}
	return result == 1, nil
}

// Rename renames a key.
func (db *EmbeddedDb) Rename(key, newkey string) error {
	if err := db.checkOpen(); err != nil {
		return err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cNewKey := C.CString(newkey)
	defer C.free(unsafe.Pointer(cNewKey))

	result := C.redlite_rename(db.handle, cKey, cNewKey)
	if result < 0 {
		return getLastError()
	}
	return nil
}

// RenameNX renames a key only if the new key doesn't exist.
func (db *EmbeddedDb) RenameNX(key, newkey string) (bool, error) {
	if err := db.checkOpen(); err != nil {
		return false, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cNewKey := C.CString(newkey)
	defer C.free(unsafe.Pointer(cNewKey))

	result := C.redlite_renamenx(db.handle, cKey, cNewKey)
	if result < 0 {
		return false, getLastError()
	}
	return result == 1, nil
}

// Keys returns all keys matching the pattern.
func (db *EmbeddedDb) Keys(pattern string) ([]string, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}

	cPattern := C.CString(pattern)
	defer C.free(unsafe.Pointer(cPattern))

	result := C.redlite_keys(db.handle, cPattern)
	return stringArrayToGo(result), nil
}

// DBSize returns the number of keys in the database.
func (db *EmbeddedDb) DBSize() (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	result := C.redlite_dbsize(db.handle)
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// FlushDB deletes all keys in the database.
func (db *EmbeddedDb) FlushDB() error {
	if err := db.checkOpen(); err != nil {
		return err
	}

	result := C.redlite_flushdb(db.handle)
	if result < 0 {
		return getLastError()
	}
	return nil
}

// Select selects a database.
func (db *EmbeddedDb) Select(dbNum int) error {
	if err := db.checkOpen(); err != nil {
		return err
	}

	result := C.redlite_select(db.handle, C.int(dbNum))
	if result < 0 {
		return getLastError()
	}
	return nil
}

// =============================================================================
// Hash Commands
// =============================================================================

// HSet sets hash fields.
func (db *EmbeddedDb) HSet(key string, fields map[string][]byte) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}
	if len(fields) == 0 {
		return 0, nil
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	// Allocate C arrays
	fieldNames := C.malloc(C.size_t(len(fields)) * C.size_t(unsafe.Sizeof(uintptr(0))))
	defer C.free(fieldNames)
	valuesPtr := C.malloc(C.size_t(len(fields)) * C.size_t(unsafe.Sizeof(C.RedliteBytes{})))
	defer C.free(valuesPtr)

	fieldSlice := (*[1 << 30]*C.char)(fieldNames)[:len(fields):len(fields)]
	valueSlice := (*[1 << 30]C.RedliteBytes)(valuesPtr)[:len(fields):len(fields)]

	i := 0
	for field, value := range fields {
		fieldSlice[i] = C.CString(field)
		defer C.free(unsafe.Pointer(fieldSlice[i]))

		if len(value) > 0 {
			cData := C.CBytes(value)
			defer C.free(cData)
			valueSlice[i] = C.RedliteBytes{
				data: (*C.uint8_t)(cData),
				len:  C.size_t(len(value)),
			}
		} else {
			valueSlice[i] = C.RedliteBytes{data: nil, len: 0}
		}
		i++
	}

	result := C.redlite_hset(db.handle, cKey, (**C.char)(fieldNames), (*C.RedliteBytes)(valuesPtr), C.size_t(len(fields)))
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// HGet gets a hash field value.
func (db *EmbeddedDb) HGet(key, field string) ([]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cField := C.CString(field)
	defer C.free(unsafe.Pointer(cField))

	result := C.redlite_hget(db.handle, cKey, cField)
	return bytesToGo(result), nil
}

// HDel deletes hash fields.
func (db *EmbeddedDb) HDel(key string, fields ...string) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}
	if len(fields) == 0 {
		return 0, nil
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	cFields := make([]*C.char, len(fields))
	for i, f := range fields {
		cFields[i] = C.CString(f)
		defer C.free(unsafe.Pointer(cFields[i]))
	}

	result := C.redlite_hdel(db.handle, cKey, &cFields[0], C.size_t(len(fields)))
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// HExists checks if a hash field exists.
func (db *EmbeddedDb) HExists(key, field string) (bool, error) {
	if err := db.checkOpen(); err != nil {
		return false, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cField := C.CString(field)
	defer C.free(unsafe.Pointer(cField))

	result := C.redlite_hexists(db.handle, cKey, cField)
	if result < 0 {
		return false, getLastError()
	}
	return result == 1, nil
}

// HLen returns the number of fields in a hash.
func (db *EmbeddedDb) HLen(key string) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_hlen(db.handle, cKey)
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// HKeys returns all field names in a hash.
func (db *EmbeddedDb) HKeys(key string) ([]string, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_hkeys(db.handle, cKey)
	return stringArrayToGo(result), nil
}

// HVals returns all values in a hash.
func (db *EmbeddedDb) HVals(key string) ([][]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_hvals(db.handle, cKey)
	return bytesArrayToGo(result), nil
}

// HIncrBy increments a hash field.
func (db *EmbeddedDb) HIncrBy(key, field string, amount int64) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cField := C.CString(field)
	defer C.free(unsafe.Pointer(cField))

	result := C.redlite_hincrby(db.handle, cKey, cField, C.int64_t(amount))
	if result == C.int64_t(math.MinInt64) {
		return 0, getLastError()
	}
	return int64(result), nil
}

// HGetAll returns all fields and values in a hash.
func (db *EmbeddedDb) HGetAll(key string) (map[string][]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_hgetall(db.handle, cKey)
	flatArray := bytesArrayToGo(result)

	// Convert flat array of field-value pairs to map
	hashMap := make(map[string][]byte)
	for i := 0; i < len(flatArray); i += 2 {
		if i+1 < len(flatArray) {
			field := string(flatArray[i])
			value := flatArray[i+1]
			hashMap[field] = value
		}
	}
	return hashMap, nil
}

// HMGet returns the values of multiple hash fields.
func (db *EmbeddedDb) HMGet(key string, fields ...string) ([][]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, nil
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	cFields := make([]*C.char, len(fields))
	for i, f := range fields {
		cFields[i] = C.CString(f)
		defer C.free(unsafe.Pointer(cFields[i]))
	}

	result := C.redlite_hmget(db.handle, cKey, &cFields[0], C.size_t(len(fields)))
	return bytesArrayToGo(result), nil
}

// =============================================================================
// List Commands
// =============================================================================

// LPush pushes values to the head of a list.
func (db *EmbeddedDb) LPush(key string, values ...[]byte) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}
	if len(values) == 0 {
		return 0, nil
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	// Allocate C array
	valuesPtr := C.malloc(C.size_t(len(values)) * C.size_t(unsafe.Sizeof(C.RedliteBytes{})))
	defer C.free(valuesPtr)
	cValues := (*[1 << 30]C.RedliteBytes)(valuesPtr)[:len(values):len(values)]

	// Track C allocations for cleanup
	cDataPtrs := make([]unsafe.Pointer, 0, len(values))
	defer func() {
		for _, p := range cDataPtrs {
			C.free(p)
		}
	}()

	for i, v := range values {
		if len(v) > 0 {
			cData := C.CBytes(v)
			cDataPtrs = append(cDataPtrs, cData)
			cValues[i] = C.RedliteBytes{
				data: (*C.uint8_t)(cData),
				len:  C.size_t(len(v)),
			}
		} else {
			cValues[i] = C.RedliteBytes{data: nil, len: 0}
		}
	}

	result := C.redlite_lpush(db.handle, cKey, (*C.RedliteBytes)(valuesPtr), C.size_t(len(values)))
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// RPush pushes values to the tail of a list.
func (db *EmbeddedDb) RPush(key string, values ...[]byte) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}
	if len(values) == 0 {
		return 0, nil
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	// Allocate C array
	valuesPtr := C.malloc(C.size_t(len(values)) * C.size_t(unsafe.Sizeof(C.RedliteBytes{})))
	defer C.free(valuesPtr)
	cValues := (*[1 << 30]C.RedliteBytes)(valuesPtr)[:len(values):len(values)]

	// Track C allocations for cleanup
	cDataPtrs := make([]unsafe.Pointer, 0, len(values))
	defer func() {
		for _, p := range cDataPtrs {
			C.free(p)
		}
	}()

	for i, v := range values {
		if len(v) > 0 {
			cData := C.CBytes(v)
			cDataPtrs = append(cDataPtrs, cData)
			cValues[i] = C.RedliteBytes{
				data: (*C.uint8_t)(cData),
				len:  C.size_t(len(v)),
			}
		} else {
			cValues[i] = C.RedliteBytes{data: nil, len: 0}
		}
	}

	result := C.redlite_rpush(db.handle, cKey, (*C.RedliteBytes)(valuesPtr), C.size_t(len(values)))
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// LPop pops values from the head of a list.
func (db *EmbeddedDb) LPop(key string, count int) ([][]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_lpop(db.handle, cKey, C.size_t(count))
	return bytesArrayToGo(result), nil
}

// RPop pops values from the tail of a list.
func (db *EmbeddedDb) RPop(key string, count int) ([][]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_rpop(db.handle, cKey, C.size_t(count))
	return bytesArrayToGo(result), nil
}

// LLen returns the length of a list.
func (db *EmbeddedDb) LLen(key string) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_llen(db.handle, cKey)
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// LRange returns a range of elements from a list.
func (db *EmbeddedDb) LRange(key string, start, stop int64) ([][]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_lrange(db.handle, cKey, C.int64_t(start), C.int64_t(stop))
	return bytesArrayToGo(result), nil
}

// LIndex returns an element from a list by index.
func (db *EmbeddedDb) LIndex(key string, index int64) ([]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_lindex(db.handle, cKey, C.int64_t(index))
	return bytesToGo(result), nil
}

// =============================================================================
// Set Commands
// =============================================================================

// SAdd adds members to a set.
func (db *EmbeddedDb) SAdd(key string, members ...[]byte) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}
	if len(members) == 0 {
		return 0, nil
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	// Allocate C array
	membersPtr := C.malloc(C.size_t(len(members)) * C.size_t(unsafe.Sizeof(C.RedliteBytes{})))
	defer C.free(membersPtr)
	cMembers := (*[1 << 30]C.RedliteBytes)(membersPtr)[:len(members):len(members)]

	cDataPtrs := make([]unsafe.Pointer, 0, len(members))
	defer func() {
		for _, p := range cDataPtrs {
			C.free(p)
		}
	}()

	for i, m := range members {
		if len(m) > 0 {
			cData := C.CBytes(m)
			cDataPtrs = append(cDataPtrs, cData)
			cMembers[i] = C.RedliteBytes{
				data: (*C.uint8_t)(cData),
				len:  C.size_t(len(m)),
			}
		} else {
			cMembers[i] = C.RedliteBytes{data: nil, len: 0}
		}
	}

	result := C.redlite_sadd(db.handle, cKey, (*C.RedliteBytes)(membersPtr), C.size_t(len(members)))
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// SRem removes members from a set.
func (db *EmbeddedDb) SRem(key string, members ...[]byte) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}
	if len(members) == 0 {
		return 0, nil
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	// Allocate C array
	membersPtr := C.malloc(C.size_t(len(members)) * C.size_t(unsafe.Sizeof(C.RedliteBytes{})))
	defer C.free(membersPtr)
	cMembers := (*[1 << 30]C.RedliteBytes)(membersPtr)[:len(members):len(members)]

	cDataPtrs := make([]unsafe.Pointer, 0, len(members))
	defer func() {
		for _, p := range cDataPtrs {
			C.free(p)
		}
	}()

	for i, m := range members {
		if len(m) > 0 {
			cData := C.CBytes(m)
			cDataPtrs = append(cDataPtrs, cData)
			cMembers[i] = C.RedliteBytes{
				data: (*C.uint8_t)(cData),
				len:  C.size_t(len(m)),
			}
		} else {
			cMembers[i] = C.RedliteBytes{data: nil, len: 0}
		}
	}

	result := C.redlite_srem(db.handle, cKey, (*C.RedliteBytes)(membersPtr), C.size_t(len(members)))
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// SMembers returns all members of a set.
func (db *EmbeddedDb) SMembers(key string) ([][]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_smembers(db.handle, cKey)
	return bytesArrayToGo(result), nil
}

// SIsMember checks if a value is a member of a set.
func (db *EmbeddedDb) SIsMember(key string, member []byte) (bool, error) {
	if err := db.checkOpen(); err != nil {
		return false, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	var dataPtr *C.uint8_t
	if len(member) > 0 {
		dataPtr = (*C.uint8_t)(unsafe.Pointer(&member[0]))
	}

	result := C.redlite_sismember(db.handle, cKey, dataPtr, C.size_t(len(member)))
	if result < 0 {
		return false, getLastError()
	}
	return result == 1, nil
}

// SCard returns the number of members in a set.
func (db *EmbeddedDb) SCard(key string) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_scard(db.handle, cKey)
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// =============================================================================
// Sorted Set Commands
// =============================================================================

// ZMemberScore represents a member with a score for sorted set operations.
type ZMemberScore struct {
	Member []byte
	Score  float64
}

// ZAdd adds members to a sorted set.
func (db *EmbeddedDb) ZAdd(key string, members ...ZMemberScore) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}
	if len(members) == 0 {
		return 0, nil
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	// Allocate C array
	membersPtr := C.malloc(C.size_t(len(members)) * C.size_t(unsafe.Sizeof(C.RedliteZMember{})))
	defer C.free(membersPtr)
	cMembers := (*[1 << 30]C.RedliteZMember)(membersPtr)[:len(members):len(members)]

	cDataPtrs := make([]unsafe.Pointer, 0, len(members))
	defer func() {
		for _, p := range cDataPtrs {
			C.free(p)
		}
	}()

	for i, m := range members {
		cData := C.CBytes(m.Member)
		cDataPtrs = append(cDataPtrs, cData)
		cMembers[i] = C.RedliteZMember{
			score:      C.double(m.Score),
			member:     (*C.uint8_t)(cData),
			member_len: C.size_t(len(m.Member)),
		}
	}

	result := C.redlite_zadd(db.handle, cKey, (*C.RedliteZMember)(membersPtr), C.size_t(len(members)))
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// ZScore returns the score of a member in a sorted set.
func (db *EmbeddedDb) ZScore(key string, member []byte) (float64, bool, error) {
	if err := db.checkOpen(); err != nil {
		return 0, false, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	var dataPtr *C.uint8_t
	if len(member) > 0 {
		dataPtr = (*C.uint8_t)(unsafe.Pointer(&member[0]))
	}

	result := C.redlite_zscore(db.handle, cKey, dataPtr, C.size_t(len(member)))
	if math.IsNaN(float64(result)) {
		return 0, false, nil
	}
	return float64(result), true, nil
}

// ZCard returns the number of members in a sorted set.
func (db *EmbeddedDb) ZCard(key string) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_zcard(db.handle, cKey)
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// ZCount counts members with scores in a range.
func (db *EmbeddedDb) ZCount(key string, min, max float64) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	result := C.redlite_zcount(db.handle, cKey, C.double(min), C.double(max))
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// ZIncrBy increments the score of a member.
func (db *EmbeddedDb) ZIncrBy(key string, increment float64, member []byte) (float64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	var dataPtr *C.uint8_t
	if len(member) > 0 {
		dataPtr = (*C.uint8_t)(unsafe.Pointer(&member[0]))
	}

	result := C.redlite_zincrby(db.handle, cKey, C.double(increment), dataPtr, C.size_t(len(member)))
	if math.IsNaN(float64(result)) {
		return 0, getLastError()
	}
	return float64(result), nil
}

// ZRem removes members from a sorted set.
func (db *EmbeddedDb) ZRem(key string, members ...[]byte) (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}
	if len(members) == 0 {
		return 0, nil
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	// Allocate C array
	membersPtr := C.malloc(C.size_t(len(members)) * C.size_t(unsafe.Sizeof(C.RedliteBytes{})))
	defer C.free(membersPtr)
	cMembers := (*[1 << 30]C.RedliteBytes)(membersPtr)[:len(members):len(members)]

	cDataPtrs := make([]unsafe.Pointer, 0, len(members))
	defer func() {
		for _, p := range cDataPtrs {
			C.free(p)
		}
	}()

	for i, m := range members {
		if len(m) > 0 {
			cData := C.CBytes(m)
			cDataPtrs = append(cDataPtrs, cData)
			cMembers[i] = C.RedliteBytes{
				data: (*C.uint8_t)(cData),
				len:  C.size_t(len(m)),
			}
		} else {
			cMembers[i] = C.RedliteBytes{data: nil, len: 0}
		}
	}

	result := C.redlite_zrem(db.handle, cKey, (*C.RedliteBytes)(membersPtr), C.size_t(len(members)))
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// ZRange returns a range of members from a sorted set.
func (db *EmbeddedDb) ZRange(key string, start, stop int64, withScores bool) ([][]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	withScoresInt := C.int(0)
	if withScores {
		withScoresInt = C.int(1)
	}

	result := C.redlite_zrange(db.handle, cKey, C.int64_t(start), C.int64_t(stop), withScoresInt)
	return bytesArrayToGo(result), nil
}

// ZRevRange returns a range of members from a sorted set in reverse order.
func (db *EmbeddedDb) ZRevRange(key string, start, stop int64, withScores bool) ([][]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	withScoresInt := C.int(0)
	if withScores {
		withScoresInt = C.int(1)
	}

	result := C.redlite_zrevrange(db.handle, cKey, C.int64_t(start), C.int64_t(stop), withScoresInt)
	return bytesArrayToGo(result), nil
}

// =============================================================================
// Server Commands
// =============================================================================

// Vacuum compacts the database.
func (db *EmbeddedDb) Vacuum() (int64, error) {
	if err := db.checkOpen(); err != nil {
		return 0, err
	}

	result := C.redlite_vacuum(db.handle)
	if result < 0 {
		return 0, getLastError()
	}
	return int64(result), nil
}

// Version returns the redlite library version.
func Version() string {
	result := C.redlite_version()
	defer C.redlite_free_string(result)
	return C.GoString(result)
}
