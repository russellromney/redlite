-- Geo data storage for GEOADD/GEOPOS/etc commands
CREATE TABLE IF NOT EXISTS geo_data (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    member TEXT NOT NULL,
    longitude REAL NOT NULL,
    latitude REAL NOT NULL,
    geohash TEXT,
    UNIQUE(key_id, member)
);
CREATE INDEX IF NOT EXISTS idx_geo_data_key ON geo_data(key_id);

-- R*Tree spatial index for efficient radius/box queries
-- Each geo_data row has a corresponding rtree entry with same id
CREATE VIRTUAL TABLE IF NOT EXISTS geo_rtree USING rtree(
    id,
    min_lon, max_lon,
    min_lat, max_lat
);
