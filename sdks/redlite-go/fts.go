package redlite

import (
	"context"
	"fmt"
	"strconv"
)

// -----------------------------------------------------------------------------
// FTS Namespace (Native redlite Full-Text Search)
// -----------------------------------------------------------------------------

// FTSNamespace provides redlite-native full-text search commands.
type FTSNamespace struct {
	client *Redlite
}

// FTSResult represents a result from a full-text search query.
type FTSResult struct {
	DB      int
	Key     string
	Content []byte
	Rank    float64
	Snippet string
}

// FTSStats contains statistics about FTS indexing.
type FTSStats struct {
	IndexedKeys  int
	TotalTokens  int
	StorageBytes int
	Configs      []map[string]interface{}
}

// FTSEnableOption configures FTS.Enable behavior.
type FTSEnableOption func(*ftsEnableConfig)

type ftsEnableConfig struct {
	global   bool
	database *int
	pattern  string
	key      string
}

// FTSEnableGlobal enables FTS for all databases.
func FTSEnableGlobal() FTSEnableOption {
	return func(c *ftsEnableConfig) {
		c.global = true
	}
}

// FTSEnableDatabase enables FTS for a specific database.
func FTSEnableDatabase(db int) FTSEnableOption {
	return func(c *ftsEnableConfig) {
		c.database = &db
	}
}

// FTSEnablePattern enables FTS for keys matching a glob pattern.
func FTSEnablePattern(pattern string) FTSEnableOption {
	return func(c *ftsEnableConfig) {
		c.pattern = pattern
	}
}

// FTSEnableKey enables FTS for a specific key.
func FTSEnableKey(key string) FTSEnableOption {
	return func(c *ftsEnableConfig) {
		c.key = key
	}
}

// Enable enables FTS indexing for keys.
func (ns *FTSNamespace) Enable(ctx context.Context, opts ...FTSEnableOption) error {
	cfg := &ftsEnableConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	args := []interface{}{"FTS", "ENABLE"}

	if cfg.global {
		args = append(args, "GLOBAL")
	} else if cfg.database != nil {
		args = append(args, "DATABASE", *cfg.database)
	} else if cfg.pattern != "" {
		args = append(args, "PATTERN", cfg.pattern)
	} else if cfg.key != "" {
		args = append(args, "KEY", cfg.key)
	} else {
		return fmt.Errorf("must specify one of: Global, Database, Pattern, or Key")
	}

	return ns.client.Do(ctx, args...).Err()
}

// Disable disables FTS indexing for keys.
func (ns *FTSNamespace) Disable(ctx context.Context, opts ...FTSEnableOption) error {
	cfg := &ftsEnableConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	args := []interface{}{"FTS", "DISABLE"}

	if cfg.global {
		args = append(args, "GLOBAL")
	} else if cfg.database != nil {
		args = append(args, "DATABASE", *cfg.database)
	} else if cfg.pattern != "" {
		args = append(args, "PATTERN", cfg.pattern)
	} else if cfg.key != "" {
		args = append(args, "KEY", cfg.key)
	} else {
		return fmt.Errorf("must specify one of: Global, Database, Pattern, or Key")
	}

	return ns.client.Do(ctx, args...).Err()
}

// FTSSearchOption configures FTS.Search behavior.
type FTSSearchOption func(*ftsSearchConfig)

type ftsSearchConfig struct {
	limit     int
	highlight bool
}

// FTSSearchLimit sets the maximum number of results.
func FTSSearchLimit(n int) FTSSearchOption {
	return func(c *ftsSearchConfig) {
		c.limit = n
	}
}

// FTSSearchHighlight enables highlighted snippets in results.
func FTSSearchHighlight() FTSSearchOption {
	return func(c *ftsSearchConfig) {
		c.highlight = true
	}
}

// Search searches indexed keys using BM25 ranking.
func (ns *FTSNamespace) Search(ctx context.Context, query string, opts ...FTSSearchOption) ([]FTSResult, error) {
	cfg := &ftsSearchConfig{limit: 10}
	for _, opt := range opts {
		opt(cfg)
	}

	args := []interface{}{"FTS", "SEARCH", query, "LIMIT", cfg.limit}
	if cfg.highlight {
		args = append(args, "HIGHLIGHT")
	}

	result, err := ns.client.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}

	var results []FTSResult
	for _, item := range result {
		if itemSlice, ok := item.([]interface{}); ok {
			results = append(results, parseFTSResult(itemSlice))
		}
	}

	return results, nil
}

// Reindex forces reindexing of a specific key.
func (ns *FTSNamespace) Reindex(ctx context.Context, key string) error {
	return ns.client.Do(ctx, "FTS", "REINDEX", key).Err()
}

// Info returns FTS indexing statistics.
func (ns *FTSNamespace) Info(ctx context.Context) (*FTSStats, error) {
	result, err := ns.client.Do(ctx, "FTS", "INFO").Slice()
	if err != nil {
		return nil, err
	}

	stats := &FTSStats{}
	kvPairs := parseKeyValuePairs(result)

	if v, ok := kvPairs["indexed_keys"]; ok {
		stats.IndexedKeys = toInt(v)
	}
	if v, ok := kvPairs["total_tokens"]; ok {
		stats.TotalTokens = toInt(v)
	}
	if v, ok := kvPairs["storage_bytes"]; ok {
		stats.StorageBytes = toInt(v)
	}

	return stats, nil
}

func parseFTSResult(data []interface{}) FTSResult {
	result := FTSResult{}
	kvPairs := parseKeyValuePairs(data)

	if v, ok := kvPairs["db"]; ok {
		result.DB = toInt(v)
	}
	if v, ok := kvPairs["key"]; ok {
		result.Key = toString(v)
	}
	if v, ok := kvPairs["content"]; ok {
		result.Content = toBytes(v)
	}
	if v, ok := kvPairs["rank"]; ok {
		result.Rank = toFloat64(v)
	}
	if v, ok := kvPairs["snippet"]; ok {
		result.Snippet = toString(v)
	}

	return result
}

// -----------------------------------------------------------------------------
// FT Namespace (RediSearch-compatible)
// -----------------------------------------------------------------------------

// FTNamespace provides RediSearch-compatible FT.* commands.
type FTNamespace struct {
	client *Redlite
}

// FTSearchResult represents a result from FT.SEARCH.
type FTSearchResult struct {
	Total int64
	Docs  []map[string]interface{}
}

// FTCreateOption configures FT.Create behavior.
type FTCreateOption func(*ftCreateConfig)

type ftCreateConfig struct {
	on       string
	prefixes []string
}

// FTCreateOn sets the data structure type ("HASH" or "JSON").
func FTCreateOn(structType string) FTCreateOption {
	return func(c *ftCreateConfig) {
		c.on = structType
	}
}

// FTCreatePrefix sets the key prefixes to index.
func FTCreatePrefix(prefixes ...string) FTCreateOption {
	return func(c *ftCreateConfig) {
		c.prefixes = prefixes
	}
}

// Create creates a search index.
func (ns *FTNamespace) Create(ctx context.Context, index string, schema map[string]string, opts ...FTCreateOption) error {
	cfg := &ftCreateConfig{on: "HASH"}
	for _, opt := range opts {
		opt(cfg)
	}

	args := []interface{}{"FT.CREATE", index, "ON", cfg.on}

	if len(cfg.prefixes) > 0 {
		args = append(args, "PREFIX", len(cfg.prefixes))
		for _, p := range cfg.prefixes {
			args = append(args, p)
		}
	}

	args = append(args, "SCHEMA")
	for field, fieldType := range schema {
		args = append(args, field, fieldType)
	}

	return ns.client.Do(ctx, args...).Err()
}

// FTSearchOption configures FT.Search behavior.
type FTSearchOption func(*ftSearchConfig)

type ftSearchConfig struct {
	noContent    bool
	withScores   bool
	limit        [2]int
	sortBy       string
	sortOrder    string
	filter       []ftFilter
	inKeys       []string
	inFields     []string
	returnFields []string
}

type ftFilter struct {
	field string
	min   float64
	max   float64
}

// FTSearchNoContent returns only document IDs.
func FTSearchNoContent() FTSearchOption {
	return func(c *ftSearchConfig) {
		c.noContent = true
	}
}

// FTSearchWithScores includes relevance scores.
func FTSearchWithScores() FTSearchOption {
	return func(c *ftSearchConfig) {
		c.withScores = true
	}
}

// FTSearchLimit sets pagination (offset, count).
func FTSearchLimit(offset, count int) FTSearchOption {
	return func(c *ftSearchConfig) {
		c.limit = [2]int{offset, count}
	}
}

// FTSearchSortBy sets sort field and order.
func FTSearchSortBy(field, order string) FTSearchOption {
	return func(c *ftSearchConfig) {
		c.sortBy = field
		c.sortOrder = order
	}
}

// FTSearchFilter adds a numeric filter.
func FTSearchFilter(field string, min, max float64) FTSearchOption {
	return func(c *ftSearchConfig) {
		c.filter = append(c.filter, ftFilter{field, min, max})
	}
}

// FTSearchReturn specifies fields to return.
func FTSearchReturn(fields ...string) FTSearchOption {
	return func(c *ftSearchConfig) {
		c.returnFields = fields
	}
}

// Search searches the index.
func (ns *FTNamespace) Search(ctx context.Context, index, query string, opts ...FTSearchOption) (*FTSearchResult, error) {
	cfg := &ftSearchConfig{limit: [2]int{0, 10}}
	for _, opt := range opts {
		opt(cfg)
	}

	args := []interface{}{"FT.SEARCH", index, query}

	if cfg.noContent {
		args = append(args, "NOCONTENT")
	}
	if cfg.withScores {
		args = append(args, "WITHSCORES")
	}
	for _, f := range cfg.filter {
		args = append(args, "FILTER", f.field, f.min, f.max)
	}
	if len(cfg.returnFields) > 0 {
		args = append(args, "RETURN", len(cfg.returnFields))
		for _, f := range cfg.returnFields {
			args = append(args, f)
		}
	}
	if cfg.sortBy != "" {
		args = append(args, "SORTBY", cfg.sortBy, cfg.sortOrder)
	}
	args = append(args, "LIMIT", cfg.limit[0], cfg.limit[1])

	result, err := ns.client.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}

	return parseFTSearchResult(result, cfg.withScores, cfg.noContent), nil
}

func parseFTSearchResult(response []interface{}, withScores, noContent bool) *FTSearchResult {
	if len(response) == 0 {
		return &FTSearchResult{Total: 0, Docs: nil}
	}

	total := toInt64(response[0])
	var docs []map[string]interface{}

	i := 1
	for i < len(response) {
		doc := make(map[string]interface{})
		doc["id"] = toString(response[i])
		i++

		if withScores && i < len(response) {
			doc["score"] = toFloat64(response[i])
			i++
		}

		if !noContent && i < len(response) {
			if fields, ok := response[i].([]interface{}); ok {
				for j := 0; j < len(fields)-1; j += 2 {
					fieldName := toString(fields[j])
					doc[fieldName] = fields[j+1]
				}
			}
			i++
		}

		docs = append(docs, doc)
	}

	return &FTSearchResult{Total: total, Docs: docs}
}

// Aggregate runs an aggregation query.
func (ns *FTNamespace) Aggregate(ctx context.Context, index, query string, opts ...interface{}) ([]map[string]interface{}, error) {
	args := []interface{}{"FT.AGGREGATE", index, query}
	args = append(args, opts...)

	result, err := ns.client.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for i := 1; i < len(result); i++ {
		if row, ok := result[i].([]interface{}); ok {
			results = append(results, parseKeyValuePairs(row))
		}
	}

	return results, nil
}

// DropIndex drops an index.
func (ns *FTNamespace) DropIndex(ctx context.Context, index string, deleteDocuments bool) error {
	args := []interface{}{"FT.DROPINDEX", index}
	if deleteDocuments {
		args = append(args, "DD")
	}
	return ns.client.Do(ctx, args...).Err()
}

// Info returns index information.
func (ns *FTNamespace) Info(ctx context.Context, index string) (map[string]interface{}, error) {
	result, err := ns.client.Do(ctx, "FT.INFO", index).Slice()
	if err != nil {
		return nil, err
	}
	return parseKeyValuePairs(result), nil
}

// List returns all index names.
func (ns *FTNamespace) List(ctx context.Context) ([]string, error) {
	result, err := ns.client.Do(ctx, "FT._LIST").Slice()
	if err != nil {
		return nil, err
	}
	return interfaceSliceToStrings(result), nil
}

// Explain returns the execution plan for a query.
func (ns *FTNamespace) Explain(ctx context.Context, index, query string) (string, error) {
	return ns.client.Do(ctx, "FT.EXPLAIN", index, query).Text()
}

// Profile profiles a search query.
func (ns *FTNamespace) Profile(ctx context.Context, index, query string, limited bool) (map[string]interface{}, error) {
	args := []interface{}{"FT.PROFILE", index}
	if limited {
		args = append(args, "LIMITED")
	}
	args = append(args, "SEARCH", "QUERY", query)

	result, err := ns.client.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{"raw": result}, nil
}

// AliasAdd adds an alias to an index.
func (ns *FTNamespace) AliasAdd(ctx context.Context, alias, index string) error {
	return ns.client.Do(ctx, "FT.ALIASADD", alias, index).Err()
}

// AliasDel removes an alias.
func (ns *FTNamespace) AliasDel(ctx context.Context, alias string) error {
	return ns.client.Do(ctx, "FT.ALIASDEL", alias).Err()
}

// AliasUpdate updates an alias to point to a different index.
func (ns *FTNamespace) AliasUpdate(ctx context.Context, alias, index string) error {
	return ns.client.Do(ctx, "FT.ALIASUPDATE", alias, index).Err()
}

// Alter adds a field to an existing index schema.
func (ns *FTNamespace) Alter(ctx context.Context, index string, field, fieldType string) error {
	return ns.client.Do(ctx, "FT.ALTER", index, "SCHEMA", "ADD", field, fieldType).Err()
}

// SugAdd adds a suggestion to an auto-complete dictionary.
func (ns *FTNamespace) SugAdd(ctx context.Context, key, str string, score float64) (int64, error) {
	return ns.client.Do(ctx, "FT.SUGADD", key, str, score).Int64()
}

// SugDel deletes a suggestion from an auto-complete dictionary.
func (ns *FTNamespace) SugDel(ctx context.Context, key, str string) (int64, error) {
	return ns.client.Do(ctx, "FT.SUGDEL", key, str).Int64()
}

// SugGet gets completion suggestions.
func (ns *FTNamespace) SugGet(ctx context.Context, key, prefix string, opts ...interface{}) ([]string, error) {
	args := []interface{}{"FT.SUGGET", key, prefix}
	args = append(args, opts...)

	result, err := ns.client.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}

	return interfaceSliceToStrings(result), nil
}

// SugLen returns the size of an auto-complete dictionary.
func (ns *FTNamespace) SugLen(ctx context.Context, key string) (int64, error) {
	return ns.client.Do(ctx, "FT.SUGLEN", key).Int64()
}

// SynDump dumps synonym groups.
func (ns *FTNamespace) SynDump(ctx context.Context, index string) (map[string]interface{}, error) {
	result, err := ns.client.Do(ctx, "FT.SYNDUMP", index).Slice()
	if err != nil {
		return nil, err
	}
	return parseKeyValuePairs(result), nil
}

// SynUpdate updates a synonym group.
func (ns *FTNamespace) SynUpdate(ctx context.Context, index, groupID string, terms ...string) error {
	args := []interface{}{"FT.SYNUPDATE", index, groupID}
	for _, t := range terms {
		args = append(args, t)
	}
	return ns.client.Do(ctx, args...).Err()
}

// -----------------------------------------------------------------------------
// Helper type conversion functions
// -----------------------------------------------------------------------------

func toInt(v interface{}) int {
	switch val := v.(type) {
	case int:
		return val
	case int64:
		return int(val)
	case string:
		n, _ := strconv.Atoi(val)
		return n
	case []byte:
		n, _ := strconv.Atoi(string(val))
		return n
	default:
		return 0
	}
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case string:
		n, _ := strconv.ParseInt(val, 10, 64)
		return n
	case []byte:
		n, _ := strconv.ParseInt(string(val), 10, 64)
		return n
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int:
		return float64(val)
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	case []byte:
		f, _ := strconv.ParseFloat(string(val), 64)
		return f
	default:
		return 0
	}
}

func toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func toBytes(v interface{}) []byte {
	switch val := v.(type) {
	case []byte:
		return val
	case string:
		return []byte(val)
	default:
		return []byte(fmt.Sprintf("%v", val))
	}
}
