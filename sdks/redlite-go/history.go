package redlite

import (
	"context"
	"fmt"
	"time"
)

// -----------------------------------------------------------------------------
// History Namespace
// -----------------------------------------------------------------------------

// HistoryNamespace provides history tracking commands for redlite.
type HistoryNamespace struct {
	client *Redlite
}

// RetentionType specifies the type of retention policy.
type RetentionType string

const (
	RetentionUnlimited RetentionType = "unlimited"
	RetentionTime      RetentionType = "time"
	RetentionCount     RetentionType = "count"
)

// Retention represents a retention policy configuration.
type Retention struct {
	Type  RetentionType
	Value int64
}

// RetentionUnlimitedPolicy creates an unlimited retention policy.
func RetentionUnlimitedPolicy() Retention {
	return Retention{Type: RetentionUnlimited}
}

// RetentionTimePolicy creates a time-based retention policy.
func RetentionTimePolicy(duration time.Duration) Retention {
	return Retention{Type: RetentionTime, Value: duration.Milliseconds()}
}

// RetentionCountPolicy creates a count-based retention policy.
func RetentionCountPolicy(count int64) Retention {
	return Retention{Type: RetentionCount, Value: count}
}

// HistoryEntry represents a single history entry for a key.
type HistoryEntry struct {
	DB          int
	Key         string
	KeyType     string
	Version     int64
	Operation   string
	TimestampMs int64
	Data        []byte
	ExpireAt    *int64
}

// Timestamp returns the timestamp as a time.Time.
func (e HistoryEntry) Timestamp() time.Time {
	return time.UnixMilli(e.TimestampMs)
}

// HistoryStats contains statistics about history storage.
type HistoryStats struct {
	TotalEntries    int64
	OldestTimestamp *int64
	NewestTimestamp *int64
	StorageBytes    int64
}

// OldestTime returns the oldest timestamp as time.Time.
func (s HistoryStats) OldestTime() *time.Time {
	if s.OldestTimestamp == nil {
		return nil
	}
	t := time.UnixMilli(*s.OldestTimestamp)
	return &t
}

// NewestTime returns the newest timestamp as time.Time.
func (s HistoryStats) NewestTime() *time.Time {
	if s.NewestTimestamp == nil {
		return nil
	}
	t := time.UnixMilli(*s.NewestTimestamp)
	return &t
}

// HistoryEnableOption configures History.Enable behavior.
type HistoryEnableOption func(*historyEnableConfig)

type historyEnableConfig struct {
	global    bool
	database  *int
	key       string
	retention *Retention
}

// HistoryEnableGlobal enables history for all databases.
func HistoryEnableGlobal() HistoryEnableOption {
	return func(c *historyEnableConfig) {
		c.global = true
	}
}

// HistoryEnableDatabase enables history for a specific database.
func HistoryEnableDatabase(db int) HistoryEnableOption {
	return func(c *historyEnableConfig) {
		c.database = &db
	}
}

// HistoryEnableKey enables history for a specific key.
func HistoryEnableKey(key string) HistoryEnableOption {
	return func(c *historyEnableConfig) {
		c.key = key
	}
}

// HistoryEnableRetention sets the retention policy.
func HistoryEnableRetention(r Retention) HistoryEnableOption {
	return func(c *historyEnableConfig) {
		c.retention = &r
	}
}

// Enable enables history tracking.
func (ns *HistoryNamespace) Enable(ctx context.Context, opts ...HistoryEnableOption) error {
	cfg := &historyEnableConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	args := []interface{}{"HISTORY", "ENABLE"}

	if cfg.global {
		args = append(args, "GLOBAL")
	} else if cfg.database != nil {
		args = append(args, "DATABASE", *cfg.database)
	} else if cfg.key != "" {
		args = append(args, "KEY", cfg.key)
	} else {
		return fmt.Errorf("must specify one of: Global, Database, or Key")
	}

	if cfg.retention != nil {
		switch cfg.retention.Type {
		case RetentionTime:
			args = append(args, "TIME", cfg.retention.Value)
		case RetentionCount:
			args = append(args, "COUNT", cfg.retention.Value)
		}
	}

	return ns.client.Do(ctx, args...).Err()
}

// Disable disables history tracking.
func (ns *HistoryNamespace) Disable(ctx context.Context, opts ...HistoryEnableOption) error {
	cfg := &historyEnableConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	args := []interface{}{"HISTORY", "DISABLE"}

	if cfg.global {
		args = append(args, "GLOBAL")
	} else if cfg.database != nil {
		args = append(args, "DATABASE", *cfg.database)
	} else if cfg.key != "" {
		args = append(args, "KEY", cfg.key)
	} else {
		return fmt.Errorf("must specify one of: Global, Database, or Key")
	}

	return ns.client.Do(ctx, args...).Err()
}

// HistoryGetOption configures History.Get behavior.
type HistoryGetOption func(*historyGetConfig)

type historyGetConfig struct {
	version   *int64
	limit     int
	startTime *int64
	endTime   *int64
}

// HistoryGetVersion retrieves a specific version.
func HistoryGetVersion(v int64) HistoryGetOption {
	return func(c *historyGetConfig) {
		c.version = &v
	}
}

// HistoryGetLimit sets the maximum number of entries.
func HistoryGetLimit(n int) HistoryGetOption {
	return func(c *historyGetConfig) {
		c.limit = n
	}
}

// HistoryGetStartTime filters entries after this timestamp.
func HistoryGetStartTime(t time.Time) HistoryGetOption {
	return func(c *historyGetConfig) {
		ms := t.UnixMilli()
		c.startTime = &ms
	}
}

// HistoryGetEndTime filters entries before this timestamp.
func HistoryGetEndTime(t time.Time) HistoryGetOption {
	return func(c *historyGetConfig) {
		ms := t.UnixMilli()
		c.endTime = &ms
	}
}

// Get retrieves history entries for a key.
func (ns *HistoryNamespace) Get(ctx context.Context, key string, opts ...HistoryGetOption) ([]HistoryEntry, error) {
	cfg := &historyGetConfig{limit: 100}
	for _, opt := range opts {
		opt(cfg)
	}

	args := []interface{}{"HISTORY", "GET", key}

	if cfg.version != nil {
		args = append(args, "VERSION", *cfg.version)
	} else {
		args = append(args, "LIMIT", cfg.limit)
	}

	if cfg.startTime != nil {
		args = append(args, "START", *cfg.startTime)
	}
	if cfg.endTime != nil {
		args = append(args, "END", *cfg.endTime)
	}

	result, err := ns.client.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}

	var entries []HistoryEntry
	for _, item := range result {
		if itemSlice, ok := item.([]interface{}); ok {
			entries = append(entries, parseHistoryEntry(itemSlice))
		}
	}

	return entries, nil
}

// GetAt retrieves the key's value at a specific point in time.
func (ns *HistoryNamespace) GetAt(ctx context.Context, key string, timestamp time.Time) (*HistoryEntry, error) {
	ms := timestamp.UnixMilli()
	result, err := ns.client.Do(ctx, "HISTORY", "GET", key, "AT", ms).Slice()
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, nil
	}

	if itemSlice, ok := result[0].([]interface{}); ok {
		entry := parseHistoryEntry(itemSlice)
		return &entry, nil
	}

	return nil, nil
}

// VersionSummary represents a summary of a version.
type VersionSummary struct {
	Version     int64
	Operation   string
	TimestampMs int64
}

// List returns version summaries for a key.
func (ns *HistoryNamespace) List(ctx context.Context, key string) ([]VersionSummary, error) {
	result, err := ns.client.Do(ctx, "HISTORY", "LIST", key).Slice()
	if err != nil {
		return nil, err
	}

	var versions []VersionSummary
	for _, item := range result {
		if itemSlice, ok := item.([]interface{}); ok && len(itemSlice) >= 3 {
			versions = append(versions, VersionSummary{
				Version:     toInt64(itemSlice[0]),
				Operation:   toString(itemSlice[1]),
				TimestampMs: toInt64(itemSlice[2]),
			})
		}
	}

	return versions, nil
}

// Stats returns history statistics.
func (ns *HistoryNamespace) Stats(ctx context.Context, key ...string) (*HistoryStats, error) {
	args := []interface{}{"HISTORY", "STATS"}
	if len(key) > 0 {
		args = append(args, key[0])
	}

	result, err := ns.client.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}

	stats := &HistoryStats{}
	kvPairs := parseKeyValuePairs(result)

	if v, ok := kvPairs["total_entries"]; ok {
		stats.TotalEntries = toInt64(v)
	}
	if v, ok := kvPairs["oldest_timestamp"]; ok {
		ts := toInt64(v)
		if ts > 0 {
			stats.OldestTimestamp = &ts
		}
	}
	if v, ok := kvPairs["newest_timestamp"]; ok {
		ts := toInt64(v)
		if ts > 0 {
			stats.NewestTimestamp = &ts
		}
	}
	if v, ok := kvPairs["storage_bytes"]; ok {
		stats.StorageBytes = toInt64(v)
	}

	return stats, nil
}

// Clear clears history for a key.
func (ns *HistoryNamespace) Clear(ctx context.Context, key string, before ...time.Time) (int64, error) {
	args := []interface{}{"HISTORY", "CLEAR", key}

	if len(before) > 0 {
		args = append(args, "BEFORE", before[0].UnixMilli())
	}

	return ns.client.Do(ctx, args...).Int64()
}

// Prune removes all history entries before a timestamp.
func (ns *HistoryNamespace) Prune(ctx context.Context, before time.Time) (int64, error) {
	return ns.client.Do(ctx, "HISTORY", "PRUNE", "BEFORE", before.UnixMilli()).Int64()
}

// Info returns history configuration information.
func (ns *HistoryNamespace) Info(ctx context.Context) (map[string]interface{}, error) {
	result, err := ns.client.Do(ctx, "HISTORY", "INFO").Slice()
	if err != nil {
		return nil, err
	}
	return parseKeyValuePairs(result), nil
}

// Restore restores a key to a previous version.
func (ns *HistoryNamespace) Restore(ctx context.Context, key string, version int64) error {
	return ns.client.Do(ctx, "HISTORY", "RESTORE", key, "VERSION", version).Err()
}

func parseHistoryEntry(data []interface{}) HistoryEntry {
	entry := HistoryEntry{}
	if len(data) >= 8 {
		entry.DB = toInt(data[0])
		entry.Key = toString(data[1])
		entry.KeyType = toString(data[2])
		entry.Version = toInt64(data[3])
		entry.Operation = toString(data[4])
		entry.TimestampMs = toInt64(data[5])
		entry.Data = toBytes(data[6])
		if data[7] != nil {
			expireAt := toInt64(data[7])
			entry.ExpireAt = &expireAt
		}
	}
	return entry
}
