package redlite

import (
	"context"
	"encoding/json"
	"fmt"
)

// -----------------------------------------------------------------------------
// Vector Namespace
// -----------------------------------------------------------------------------

// VectorNamespace provides vector similarity search commands for redlite.
type VectorNamespace struct {
	client *Redlite
}

// DistanceMetric specifies the distance metric for vector similarity.
type DistanceMetric string

const (
	DistanceL2     DistanceMetric = "L2"
	DistanceCosine DistanceMetric = "COSINE"
	DistanceIP     DistanceMetric = "IP"
)

// VectorSearchResult represents a result from a vector similarity search.
type VectorSearchResult struct {
	VectorID string
	Distance float64
	Metadata map[string]interface{}
}

// VectorInfo contains information about a vector collection.
type VectorInfo struct {
	Key        string
	Dimensions int
	Count      int
	Metric     DistanceMetric
}

// VectorStats contains statistics about vector storage.
type VectorStats struct {
	TotalVectors int64
	TotalKeys    int64
	StorageBytes int64
	Configs      []map[string]interface{}
}

// VectorEnableOption configures Vector.Enable behavior.
type VectorEnableOption func(*vectorEnableConfig)

type vectorEnableConfig struct {
	global   bool
	database *int
	pattern  string
	key      string
	metric   DistanceMetric
}

// VectorEnableGlobal enables vectors for all databases.
func VectorEnableGlobal() VectorEnableOption {
	return func(c *vectorEnableConfig) {
		c.global = true
	}
}

// VectorEnableDatabase enables vectors for a specific database.
func VectorEnableDatabase(db int) VectorEnableOption {
	return func(c *vectorEnableConfig) {
		c.database = &db
	}
}

// VectorEnablePattern enables vectors for keys matching a glob pattern.
func VectorEnablePattern(pattern string) VectorEnableOption {
	return func(c *vectorEnableConfig) {
		c.pattern = pattern
	}
}

// VectorEnableKey enables vectors for a specific key.
func VectorEnableKey(key string) VectorEnableOption {
	return func(c *vectorEnableConfig) {
		c.key = key
	}
}

// VectorEnableMetric sets the distance metric.
func VectorEnableMetric(m DistanceMetric) VectorEnableOption {
	return func(c *vectorEnableConfig) {
		c.metric = m
	}
}

// Enable enables vector storage for keys.
func (ns *VectorNamespace) Enable(ctx context.Context, dimensions int, opts ...VectorEnableOption) error {
	cfg := &vectorEnableConfig{metric: DistanceCosine}
	for _, opt := range opts {
		opt(cfg)
	}

	args := []interface{}{"VECTOR", "ENABLE"}

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

	args = append(args, dimensions)

	if cfg.metric != DistanceCosine {
		args = append(args, "METRIC", string(cfg.metric))
	}

	return ns.client.Do(ctx, args...).Err()
}

// Disable disables vector storage for keys.
func (ns *VectorNamespace) Disable(ctx context.Context, opts ...VectorEnableOption) error {
	cfg := &vectorEnableConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	args := []interface{}{"VECTOR", "DISABLE"}

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

// VectorAddOption configures Vector.Add behavior.
type VectorAddOption func(*vectorAddConfig)

type vectorAddConfig struct {
	metadata map[string]interface{}
	nx       bool
}

// VectorAddMetadata sets metadata to store with the vector.
func VectorAddMetadata(metadata map[string]interface{}) VectorAddOption {
	return func(c *vectorAddConfig) {
		c.metadata = metadata
	}
}

// VectorAddNX only adds if the element doesn't exist.
func VectorAddNX() VectorAddOption {
	return func(c *vectorAddConfig) {
		c.nx = true
	}
}

// Add adds a vector to a collection.
func (ns *VectorNamespace) Add(ctx context.Context, key, element string, vector []float64, opts ...VectorAddOption) error {
	cfg := &vectorAddConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	args := []interface{}{"VADD", key, element}

	// Add vector values
	for _, v := range vector {
		args = append(args, v)
	}

	// Add metadata if provided
	if cfg.metadata != nil {
		jsonBytes, err := json.Marshal(cfg.metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}
		args = append(args, "ATTRIBUTES", string(jsonBytes))
	}

	if cfg.nx {
		args = append(args, "NX")
	}

	return ns.client.Do(ctx, args...).Err()
}

// VectorSearchOption configures Vector.Search behavior.
type VectorSearchOption func(*vectorSearchConfig)

type vectorSearchConfig struct {
	count        int
	withScores   bool
	withMetadata bool
	filter       string
}

// VectorSearchCount sets the number of results to return.
func VectorSearchCount(n int) VectorSearchOption {
	return func(c *vectorSearchConfig) {
		c.count = n
	}
}

// VectorSearchWithScores includes distance scores.
func VectorSearchWithScores() VectorSearchOption {
	return func(c *vectorSearchConfig) {
		c.withScores = true
	}
}

// VectorSearchWithMetadata includes metadata in results.
func VectorSearchWithMetadata() VectorSearchOption {
	return func(c *vectorSearchConfig) {
		c.withMetadata = true
	}
}

// VectorSearchFilter sets a metadata filter expression.
func VectorSearchFilter(filter string) VectorSearchOption {
	return func(c *vectorSearchConfig) {
		c.filter = filter
	}
}

// Search searches for similar vectors.
func (ns *VectorNamespace) Search(ctx context.Context, key string, vector []float64, opts ...VectorSearchOption) ([]VectorSearchResult, error) {
	cfg := &vectorSearchConfig{count: 10, withScores: true}
	for _, opt := range opts {
		opt(cfg)
	}

	args := []interface{}{"VSIM", key}

	// Add query vector
	for _, v := range vector {
		args = append(args, v)
	}

	args = append(args, "COUNT", cfg.count)

	if cfg.withScores {
		args = append(args, "WITHSCORES")
	}

	if cfg.withMetadata {
		args = append(args, "WITHATTRIBUTES")
	}

	if cfg.filter != "" {
		args = append(args, "FILTER", cfg.filter)
	}

	result, err := ns.client.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}

	var results []VectorSearchResult
	for _, item := range result {
		switch v := item.(type) {
		case []interface{}:
			results = append(results, parseVectorSearchResult(v))
		case string:
			results = append(results, VectorSearchResult{VectorID: v})
		case []byte:
			results = append(results, VectorSearchResult{VectorID: string(v)})
		}
	}

	return results, nil
}

// Remove removes vectors from a collection.
func (ns *VectorNamespace) Remove(ctx context.Context, key string, elements ...string) (int64, error) {
	if len(elements) == 0 {
		return 0, fmt.Errorf("must specify at least one element to remove")
	}

	args := []interface{}{"VREM", key}
	for _, e := range elements {
		args = append(args, e)
	}

	return ns.client.Do(ctx, args...).Int64()
}

// Get retrieves a vector by element ID.
func (ns *VectorNamespace) Get(ctx context.Context, key, element string, withMetadata bool) ([]float64, error) {
	args := []interface{}{"VEMB", key, element}

	if withMetadata {
		args = append(args, "WITHATTRIBUTES")
	}

	result, err := ns.client.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, nil
	}

	vector := make([]float64, len(result))
	for i, v := range result {
		vector[i] = toFloat64(v)
	}

	return vector, nil
}

// Card returns the number of vectors in a collection.
func (ns *VectorNamespace) Card(ctx context.Context, key string) (int64, error) {
	return ns.client.Do(ctx, "VCARD", key).Int64()
}

// Dim returns the dimensions of vectors in a collection.
func (ns *VectorNamespace) Dim(ctx context.Context, key string) (int64, error) {
	return ns.client.Do(ctx, "VDIM", key).Int64()
}

// Info returns vector storage information.
func (ns *VectorNamespace) Info(ctx context.Context, key ...string) (interface{}, error) {
	var args []interface{}
	if len(key) > 0 {
		args = []interface{}{"VINFO", key[0]}
	} else {
		args = []interface{}{"VECTOR", "INFO"}
	}

	result, err := ns.client.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}

	kvPairs := parseKeyValuePairs(result)

	if len(key) > 0 {
		metricStr := "COSINE"
		if v, ok := kvPairs["metric"]; ok {
			metricStr = toString(v)
		}

		return &VectorInfo{
			Key:        key[0],
			Dimensions: toInt(kvPairs["dimensions"]),
			Count:      toInt(kvPairs["count"]),
			Metric:     DistanceMetric(metricStr),
		}, nil
	}

	var configs []map[string]interface{}
	if v, ok := kvPairs["configs"]; ok {
		if configsSlice, ok := v.([]interface{}); ok {
			for _, c := range configsSlice {
				if cm, ok := c.(map[string]interface{}); ok {
					configs = append(configs, cm)
				}
			}
		}
	}

	return &VectorStats{
		TotalVectors: toInt64(kvPairs["total_vectors"]),
		TotalKeys:    toInt64(kvPairs["total_keys"]),
		StorageBytes: toInt64(kvPairs["storage_bytes"]),
		Configs:      configs,
	}, nil
}

// ListResult contains the result of a vector list operation.
type ListResult struct {
	NextCursor int64
	Elements   []string
}

// List lists vector element IDs in a collection.
func (ns *VectorNamespace) List(ctx context.Context, key string, count int, cursor int64) (*ListResult, error) {
	result, err := ns.client.Do(ctx, "VLIST", key, "COUNT", count, "CURSOR", cursor).Slice()
	if err != nil {
		return nil, err
	}

	listResult := &ListResult{}
	if len(result) >= 1 {
		listResult.NextCursor = toInt64(result[0])
	}
	if len(result) >= 2 {
		if elements, ok := result[1].([]interface{}); ok {
			listResult.Elements = interfaceSliceToStrings(elements)
		}
	}

	return listResult, nil
}

// Random returns random vector element IDs from a collection.
func (ns *VectorNamespace) Random(ctx context.Context, key string, count int) ([]string, error) {
	result, err := ns.client.Do(ctx, "VRANDMEMBER", key, count).Slice()
	if err != nil {
		return nil, err
	}

	return interfaceSliceToStrings(result), nil
}

// GetAttr returns the attributes/metadata for a vector element.
func (ns *VectorNamespace) GetAttr(ctx context.Context, key, element string) (map[string]interface{}, error) {
	result, err := ns.client.Do(ctx, "VGETATTR", key, element).Text()
	if err != nil {
		return nil, err
	}

	var metadata map[string]interface{}
	if err := json.Unmarshal([]byte(result), &metadata); err != nil {
		return nil, err
	}

	return metadata, nil
}

// SetAttr sets the attributes/metadata for a vector element.
func (ns *VectorNamespace) SetAttr(ctx context.Context, key, element string, metadata map[string]interface{}) error {
	jsonBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	return ns.client.Do(ctx, "VSETATTR", key, element, string(jsonBytes)).Err()
}

func parseVectorSearchResult(data []interface{}) VectorSearchResult {
	result := VectorSearchResult{}

	if len(data) >= 1 {
		result.VectorID = toString(data[0])
	}
	if len(data) >= 2 {
		result.Distance = toFloat64(data[1])
	}
	if len(data) >= 3 && data[2] != nil {
		switch v := data[2].(type) {
		case []byte:
			var metadata map[string]interface{}
			if err := json.Unmarshal(v, &metadata); err == nil {
				result.Metadata = metadata
			}
		case string:
			var metadata map[string]interface{}
			if err := json.Unmarshal([]byte(v), &metadata); err == nil {
				result.Metadata = metadata
			}
		case map[string]interface{}:
			result.Metadata = v
		}
	}

	return result
}
