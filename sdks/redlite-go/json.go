package redlite

import (
	"context"
)

// -----------------------------------------------------------------------------
// JSON Namespace (ReJSON-compatible commands)
// -----------------------------------------------------------------------------

// JSONNamespace provides ReJSON-compatible commands for redlite.
type JSONNamespace struct {
	client *Redlite
}

// Set sets a JSON value at path. Returns true on success.
// Use NX/XX options via functional options.
func (ns *JSONNamespace) Set(ctx context.Context, key, path, value string, opts ...JSONSetOption) (bool, error) {
	cfg := &jsonSetConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	args := []interface{}{"JSON.SET", key, path, value}

	if cfg.nx {
		args = append(args, "NX")
	} else if cfg.xx {
		args = append(args, "XX")
	}

	result, err := ns.client.Do(ctx, args...).Text()
	if err != nil {
		return false, err
	}
	return result == "OK", nil
}

// JSONSetOption configures JSON.Set behavior.
type JSONSetOption func(*jsonSetConfig)

type jsonSetConfig struct {
	nx bool
	xx bool
}

// JSONSetNX only sets if the key/path does not exist.
func JSONSetNX() JSONSetOption {
	return func(c *jsonSetConfig) {
		c.nx = true
	}
}

// JSONSetXX only sets if the key/path already exists.
func JSONSetXX() JSONSetOption {
	return func(c *jsonSetConfig) {
		c.xx = true
	}
}

// Get retrieves JSON value at path(s).
func (ns *JSONNamespace) Get(ctx context.Context, key string, paths ...string) (string, error) {
	if len(paths) == 0 {
		paths = []string{"$"}
	}

	args := []interface{}{"JSON.GET", key}
	for _, p := range paths {
		args = append(args, p)
	}

	return ns.client.Do(ctx, args...).Text()
}

// Del deletes JSON value at path. Returns number of values deleted.
func (ns *JSONNamespace) Del(ctx context.Context, key string, path ...string) (int64, error) {
	args := []interface{}{"JSON.DEL", key}
	if len(path) > 0 {
		args = append(args, path[0])
	}
	return ns.client.Do(ctx, args...).Int64()
}

// Type returns the type of JSON value at path.
func (ns *JSONNamespace) Type(ctx context.Context, key string, path ...string) (string, error) {
	args := []interface{}{"JSON.TYPE", key}
	if len(path) > 0 {
		args = append(args, path[0])
	}
	return ns.client.Do(ctx, args...).Text()
}

// NumIncrBy increments numeric value at path. Returns new value as string.
func (ns *JSONNamespace) NumIncrBy(ctx context.Context, key, path string, increment float64) (string, error) {
	return ns.client.Do(ctx, "JSON.NUMINCRBY", key, path, increment).Text()
}

// StrAppend appends string to JSON string at path. Returns new length.
func (ns *JSONNamespace) StrAppend(ctx context.Context, key, value string, path ...string) (int64, error) {
	args := []interface{}{"JSON.STRAPPEND", key}
	if len(path) > 0 {
		args = append(args, path[0])
	}
	args = append(args, value)
	return ns.client.Do(ctx, args...).Int64()
}

// StrLen returns length of JSON string at path.
func (ns *JSONNamespace) StrLen(ctx context.Context, key string, path ...string) (int64, error) {
	args := []interface{}{"JSON.STRLEN", key}
	if len(path) > 0 {
		args = append(args, path[0])
	}
	return ns.client.Do(ctx, args...).Int64()
}

// ArrAppend appends values to JSON array. Returns new array length.
func (ns *JSONNamespace) ArrAppend(ctx context.Context, key, path string, values ...string) (int64, error) {
	args := []interface{}{"JSON.ARRAPPEND", key, path}
	for _, v := range values {
		args = append(args, v)
	}
	return ns.client.Do(ctx, args...).Int64()
}

// ArrLen returns length of JSON array at path.
func (ns *JSONNamespace) ArrLen(ctx context.Context, key string, path ...string) (int64, error) {
	args := []interface{}{"JSON.ARRLEN", key}
	if len(path) > 0 {
		args = append(args, path[0])
	}
	return ns.client.Do(ctx, args...).Int64()
}

// ArrPop pops element from JSON array. Returns the popped element.
func (ns *JSONNamespace) ArrPop(ctx context.Context, key string, path string, index ...int64) (string, error) {
	args := []interface{}{"JSON.ARRPOP", key}
	if path != "" {
		args = append(args, path)
	}
	if len(index) > 0 {
		args = append(args, index[0])
	}
	return ns.client.Do(ctx, args...).Text()
}

// ArrIndex finds the index of an element in JSON array.
func (ns *JSONNamespace) ArrIndex(ctx context.Context, key, path, value string) (int64, error) {
	return ns.client.Do(ctx, "JSON.ARRINDEX", key, path, value).Int64()
}

// ArrInsert inserts values at index in JSON array.
func (ns *JSONNamespace) ArrInsert(ctx context.Context, key, path string, index int64, values ...string) (int64, error) {
	args := []interface{}{"JSON.ARRINSERT", key, path, index}
	for _, v := range values {
		args = append(args, v)
	}
	return ns.client.Do(ctx, args...).Int64()
}

// ArrTrim trims JSON array to specified range.
func (ns *JSONNamespace) ArrTrim(ctx context.Context, key, path string, start, stop int64) (int64, error) {
	return ns.client.Do(ctx, "JSON.ARRTRIM", key, path, start, stop).Int64()
}

// Clear clears container values (arrays/objects). Returns count of cleared values.
func (ns *JSONNamespace) Clear(ctx context.Context, key string, path ...string) (int64, error) {
	args := []interface{}{"JSON.CLEAR", key}
	if len(path) > 0 {
		args = append(args, path[0])
	}
	return ns.client.Do(ctx, args...).Int64()
}

// Toggle toggles boolean value at path.
func (ns *JSONNamespace) Toggle(ctx context.Context, key, path string) (bool, error) {
	result, err := ns.client.Do(ctx, "JSON.TOGGLE", key, path).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

// MGet retrieves JSON value at path from multiple keys.
func (ns *JSONNamespace) MGet(ctx context.Context, path string, keys ...string) ([]string, error) {
	args := []interface{}{"JSON.MGET"}
	for _, k := range keys {
		args = append(args, k)
	}
	args = append(args, path)

	result, err := ns.client.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}

	var values []string
	for _, v := range result {
		values = append(values, toString(v))
	}
	return values, nil
}

// Merge merges JSON value at path.
func (ns *JSONNamespace) Merge(ctx context.Context, key, path, value string) (bool, error) {
	result, err := ns.client.Do(ctx, "JSON.MERGE", key, path, value).Text()
	if err != nil {
		return false, err
	}
	return result == "OK", nil
}
