package redlite

import (
	"context"
	"fmt"
)

// -----------------------------------------------------------------------------
// Geo Namespace
// -----------------------------------------------------------------------------

// GeoNamespace provides geospatial commands for redlite.
type GeoNamespace struct {
	client *Redlite
}

// GeoUnit specifies distance units for geo commands.
type GeoUnit string

const (
	GeoUnitMeters     GeoUnit = "m"
	GeoUnitKilometers GeoUnit = "km"
	GeoUnitMiles      GeoUnit = "mi"
	GeoUnitFeet       GeoUnit = "ft"
)

// GeoSort specifies sort order for geo searches.
type GeoSort string

const (
	GeoSortASC  GeoSort = "ASC"
	GeoSortDESC GeoSort = "DESC"
)

// GeoPosition represents a geographic position.
type GeoPosition struct {
	Longitude float64
	Latitude  float64
}

// GeoSearchResult represents a result from a geo search.
type GeoSearchResult struct {
	Member   string
	Distance *float64
	Position *GeoPosition
	Hash     *int64
}

// GeoMember represents a member with its position.
type GeoMember struct {
	Name      string
	Latitude  float64
	Longitude float64
}

// GeoAddOption configures GeoAdd behavior.
type GeoAddOption func(*geoAddConfig)

type geoAddConfig struct {
	nx bool
	xx bool
	ch bool
}

// GeoAddNX only adds new members.
func GeoAddNX() GeoAddOption {
	return func(c *geoAddConfig) {
		c.nx = true
	}
}

// GeoAddXX only updates existing members.
func GeoAddXX() GeoAddOption {
	return func(c *geoAddConfig) {
		c.xx = true
	}
}

// GeoAddCH returns the number of changed members.
func GeoAddCH() GeoAddOption {
	return func(c *geoAddConfig) {
		c.ch = true
	}
}

// Add adds geographic members to a key.
func (ns *GeoNamespace) Add(ctx context.Context, key string, members []GeoMember, opts ...GeoAddOption) (int64, error) {
	cfg := &geoAddConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	args := []interface{}{"GEOADD", key}

	if cfg.nx {
		args = append(args, "NX")
	}
	if cfg.xx {
		args = append(args, "XX")
	}
	if cfg.ch {
		args = append(args, "CH")
	}

	for _, m := range members {
		// GEOADD takes longitude first, then latitude
		args = append(args, m.Longitude, m.Latitude, m.Name)
	}

	return ns.client.Do(ctx, args...).Int64()
}

// GeoSearchOption configures GeoSearch behavior.
type GeoSearchOption func(*geoSearchConfig)

type geoSearchConfig struct {
	member    string
	longitude *float64
	latitude  *float64
	radius    *float64
	width     *float64
	height    *float64
	unit      GeoUnit
	sort      *GeoSort
	count     *int
	any       bool
	withCoord bool
	withDist  bool
	withHash  bool
}

// GeoSearchFromMember searches from an existing member's position.
func GeoSearchFromMember(member string) GeoSearchOption {
	return func(c *geoSearchConfig) {
		c.member = member
	}
}

// GeoSearchFromLonLat searches from a specific position.
func GeoSearchFromLonLat(longitude, latitude float64) GeoSearchOption {
	return func(c *geoSearchConfig) {
		c.longitude = &longitude
		c.latitude = &latitude
	}
}

// GeoSearchByRadius searches by radius.
func GeoSearchByRadius(radius float64, unit GeoUnit) GeoSearchOption {
	return func(c *geoSearchConfig) {
		c.radius = &radius
		c.unit = unit
	}
}

// GeoSearchByBox searches by box.
func GeoSearchByBox(width, height float64, unit GeoUnit) GeoSearchOption {
	return func(c *geoSearchConfig) {
		c.width = &width
		c.height = &height
		c.unit = unit
	}
}

// GeoSearchSort sets the sort order.
func GeoSearchSort(sort GeoSort) GeoSearchOption {
	return func(c *geoSearchConfig) {
		c.sort = &sort
	}
}

// GeoSearchCount sets the maximum results.
func GeoSearchCount(n int) GeoSearchOption {
	return func(c *geoSearchConfig) {
		c.count = &n
	}
}

// GeoSearchAny returns any N results, not necessarily closest.
func GeoSearchAny() GeoSearchOption {
	return func(c *geoSearchConfig) {
		c.any = true
	}
}

// GeoSearchWithCoord includes coordinates in results.
func GeoSearchWithCoord() GeoSearchOption {
	return func(c *geoSearchConfig) {
		c.withCoord = true
	}
}

// GeoSearchWithDist includes distance in results.
func GeoSearchWithDist() GeoSearchOption {
	return func(c *geoSearchConfig) {
		c.withDist = true
	}
}

// GeoSearchWithHash includes geohash in results.
func GeoSearchWithHash() GeoSearchOption {
	return func(c *geoSearchConfig) {
		c.withHash = true
	}
}

// Search searches for members within a geographic area.
func (ns *GeoNamespace) Search(ctx context.Context, key string, opts ...GeoSearchOption) ([]GeoSearchResult, error) {
	cfg := &geoSearchConfig{unit: GeoUnitMeters}
	for _, opt := range opts {
		opt(cfg)
	}

	args := []interface{}{"GEOSEARCH", key}

	// Center point
	if cfg.member != "" {
		args = append(args, "FROMMEMBER", cfg.member)
	} else if cfg.longitude != nil && cfg.latitude != nil {
		args = append(args, "FROMLONLAT", *cfg.longitude, *cfg.latitude)
	} else {
		return nil, fmt.Errorf("must specify either FromMember or FromLonLat")
	}

	// Search shape
	if cfg.radius != nil {
		args = append(args, "BYRADIUS", *cfg.radius, string(cfg.unit))
	} else if cfg.width != nil && cfg.height != nil {
		args = append(args, "BYBOX", *cfg.width, *cfg.height, string(cfg.unit))
	} else {
		return nil, fmt.Errorf("must specify either ByRadius or ByBox")
	}

	// Options
	if cfg.sort != nil {
		args = append(args, string(*cfg.sort))
	}
	if cfg.count != nil {
		args = append(args, "COUNT", *cfg.count)
		if cfg.any {
			args = append(args, "ANY")
		}
	}
	if cfg.withCoord {
		args = append(args, "WITHCOORD")
	}
	if cfg.withDist {
		args = append(args, "WITHDIST")
	}
	if cfg.withHash {
		args = append(args, "WITHHASH")
	}

	result, err := ns.client.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}

	return parseGeoSearchResults(result, cfg.withCoord, cfg.withDist, cfg.withHash), nil
}

// Distance returns the distance between two members.
func (ns *GeoNamespace) Distance(ctx context.Context, key, member1, member2 string, unit GeoUnit) (*float64, error) {
	result, err := ns.client.Do(ctx, "GEODIST", key, member1, member2, string(unit)).Float64()
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// Position returns the positions of members.
func (ns *GeoNamespace) Position(ctx context.Context, key string, members ...string) (map[string]*GeoPosition, error) {
	if len(members) == 0 {
		return map[string]*GeoPosition{}, nil
	}

	args := []interface{}{"GEOPOS", key}
	for _, m := range members {
		args = append(args, m)
	}

	result, err := ns.client.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}

	positions := make(map[string]*GeoPosition)
	for i, member := range members {
		if i < len(result) && result[i] != nil {
			if coords, ok := result[i].([]interface{}); ok && len(coords) >= 2 {
				positions[member] = &GeoPosition{
					Longitude: toFloat64(coords[0]),
					Latitude:  toFloat64(coords[1]),
				}
			} else {
				positions[member] = nil
			}
		} else {
			positions[member] = nil
		}
	}

	return positions, nil
}

// Hash returns geohash strings for members.
func (ns *GeoNamespace) Hash(ctx context.Context, key string, members ...string) (map[string]*string, error) {
	if len(members) == 0 {
		return map[string]*string{}, nil
	}

	args := []interface{}{"GEOHASH", key}
	for _, m := range members {
		args = append(args, m)
	}

	result, err := ns.client.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}

	hashes := make(map[string]*string)
	for i, member := range members {
		if i < len(result) && result[i] != nil {
			hash := toString(result[i])
			hashes[member] = &hash
		} else {
			hashes[member] = nil
		}
	}

	return hashes, nil
}

// GeoSearchStoreOption configures GeoSearchStore behavior.
type GeoSearchStoreOption func(*geoSearchStoreConfig)

type geoSearchStoreConfig struct {
	geoSearchConfig
	storeDist bool
}

// GeoSearchStoreDist stores distances instead of geohashes.
func GeoSearchStoreDist() GeoSearchStoreOption {
	return func(c *geoSearchStoreConfig) {
		c.storeDist = true
	}
}

// SearchStore searches and stores results in a sorted set.
func (ns *GeoNamespace) SearchStore(ctx context.Context, dest, key string, opts ...interface{}) (int64, error) {
	cfg := &geoSearchStoreConfig{geoSearchConfig: geoSearchConfig{unit: GeoUnitMeters}}

	// Parse options
	for _, opt := range opts {
		switch o := opt.(type) {
		case GeoSearchOption:
			o(&cfg.geoSearchConfig)
		case GeoSearchStoreOption:
			o(cfg)
		}
	}

	args := []interface{}{"GEOSEARCHSTORE", dest, key}

	// Center point
	if cfg.member != "" {
		args = append(args, "FROMMEMBER", cfg.member)
	} else if cfg.longitude != nil && cfg.latitude != nil {
		args = append(args, "FROMLONLAT", *cfg.longitude, *cfg.latitude)
	} else {
		return 0, fmt.Errorf("must specify either FromMember or FromLonLat")
	}

	// Search shape
	if cfg.radius != nil {
		args = append(args, "BYRADIUS", *cfg.radius, string(cfg.unit))
	} else if cfg.width != nil && cfg.height != nil {
		args = append(args, "BYBOX", *cfg.width, *cfg.height, string(cfg.unit))
	} else {
		return 0, fmt.Errorf("must specify either ByRadius or ByBox")
	}

	// Options
	if cfg.sort != nil {
		args = append(args, string(*cfg.sort))
	}
	if cfg.count != nil {
		args = append(args, "COUNT", *cfg.count)
	}
	if cfg.storeDist {
		args = append(args, "STOREDIST")
	}

	return ns.client.Do(ctx, args...).Int64()
}

func parseGeoSearchResults(response []interface{}, withCoord, withDist, withHash bool) []GeoSearchResult {
	var results []GeoSearchResult

	for _, item := range response {
		switch v := item.(type) {
		case string:
			results = append(results, GeoSearchResult{Member: v})
		case []byte:
			results = append(results, GeoSearchResult{Member: string(v)})
		case []interface{}:
			result := GeoSearchResult{}
			idx := 0

			if idx < len(v) {
				result.Member = toString(v[idx])
				idx++
			}

			if withDist && idx < len(v) {
				dist := toFloat64(v[idx])
				result.Distance = &dist
				idx++
			}

			if withHash && idx < len(v) {
				hash := toInt64(v[idx])
				result.Hash = &hash
				idx++
			}

			if withCoord && idx < len(v) {
				if coords, ok := v[idx].([]interface{}); ok && len(coords) >= 2 {
					result.Position = &GeoPosition{
						Longitude: toFloat64(coords[0]),
						Latitude:  toFloat64(coords[1]),
					}
				}
			}

			results = append(results, result)
		}
	}

	return results
}
