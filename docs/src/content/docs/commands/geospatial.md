---
title: Geospatial
description: Geospatial commands using R*Tree index
---

Redlite implements Redis geospatial commands using SQLite's R*Tree spatial index. Requires `--features geo` when compiling.

## Installation

```bash
cargo add redlite --features geo
# or
cargo install redlite --features geo
```

## Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| GEOADD | `GEOADD key [NX\|XX] longitude latitude member [longitude latitude member ...]` | Add geospatial items |
| GEOPOS | `GEOPOS key member [member ...]` | Get coordinates |
| GEODIST | `GEODIST key member1 member2 [M\|KM\|MI\|FT]` | Calculate distance |
| GEOHASH | `GEOHASH key member [member ...]` | Get geohash string |
| GEOSEARCH | `GEOSEARCH key (FROMMEMBER member \| FROMLONLAT longitude latitude) (BYRADIUS radius [M\|KM\|MI\|FT] \| BYBOX width height [M\|KM\|MI\|FT]) [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count]` | Search by radius or box |
| GEOSEARCHSTORE | `GEOSEARCHSTORE destination source ...` | Search and store results |

## Examples

### Add Locations

```bash
# Add cities with coordinates
127.0.0.1:6379> GEOADD locations -122.4194 37.7749 "San Francisco"
(integer) 1

127.0.0.1:6379> GEOADD locations -73.9857 40.7484 "New York"
(integer) 1

127.0.0.1:6379> GEOADD locations -0.1276 51.5074 "London"
(integer) 1
```

### Get Coordinates

```bash
127.0.0.1:6379> GEOPOS locations "San Francisco" "New York"
1) 1) "-122.41940000000001"
   2) "37.77489999999999"
2) 1) "-73.98569999999999"
   2) "40.748399999999997"
```

### Calculate Distance

```bash
# Distance in kilometers
127.0.0.1:6379> GEODIST locations "San Francisco" "New York" KM
"4138.3798"

# Distance in miles
127.0.0.1:6379> GEODIST locations "San Francisco" "New York" MI
"2571.6719"
```

### Search by Radius

```bash
# Find locations within 5000km of San Francisco
127.0.0.1:6379> GEOSEARCH locations FROMMEMBER "San Francisco" BYRADIUS 5000 KM
1) "San Francisco"
2) "New York"

# With coordinates and distance
127.0.0.1:6379> GEOSEARCH locations FROMMEMBER "San Francisco" BYRADIUS 5000 KM WITHCOORD WITHDIST
1) 1) "San Francisco"
   2) "0.0000"
   3) 1) "-122.41940000000001"
      2) "37.77489999999999"
2) 1) "New York"
   2) "4138.3798"
   3) 1) "-73.98569999999999"
      2) "40.748399999999997"
```

### Search from Coordinates

```bash
# Find locations within 1000km of coordinates
127.0.0.1:6379> GEOSEARCH locations FROMLONLAT -122.0 38.0 BYRADIUS 1000 KM
1) "San Francisco"
```

### Search by Box

```bash
# Find locations within bounding box
127.0.0.1:6379> GEOSEARCH locations FROMMEMBER "San Francisco" BYBOX 6000 6000 KM
1) "San Francisco"
2) "New York"
```

### Geohash

```bash
127.0.0.1:6379> GEOHASH locations "San Francisco" "New York"
1) "9q8yyk8y"
2) "dr5regw2"
```

## Units

Distance units supported:

| Unit | Description |
|------|-------------|
| `M` | Meters |
| `KM` | Kilometers |
| `MI` | Miles |
| `FT` | Feet |

## Library Mode (Rust)

```rust
use redlite::Db;

let db = Db::open("mydata.db")?;

// Add locations
db.geoadd("locations", &[
    (-122.4194, 37.7749, "San Francisco"),
    (-73.9857, 40.7484, "New York"),
])?;

// Get positions
let positions = db.geopos("locations", &["San Francisco", "New York"])?;

// Calculate distance (in meters by default)
let distance = db.geodist("locations", "San Francisco", "New York", None)?;

// Search by radius
let results = db.geosearch_radius(
    "locations",
    -122.4194,
    37.7749,
    5000.0,
    "KM",
    None,
)?;
```

## Use Cases

### Store Locator

```bash
# Add store locations
GEOADD stores -122.4 37.8 "Store A"
GEOADD stores -122.5 37.7 "Store B"

# Find nearest stores within 10km
GEOSEARCH stores FROMLONLAT -122.45 37.75 BYRADIUS 10 KM COUNT 5 WITHDIST
```

### Delivery Zones

```bash
# Add delivery addresses
GEOADD deliveries -122.41 37.77 "order:1"
GEOADD deliveries -122.42 37.78 "order:2"

# Find orders within driver's range
GEOSEARCH deliveries FROMMEMBER "driver:1" BYRADIUS 5 KM
```

### Location-Based Services

```bash
# Add points of interest
GEOADD poi -122.419 37.775 "Museum"
GEOADD poi -122.420 37.776 "Park"

# Find nearby attractions
GEOSEARCH poi FROMLONLAT -122.42 37.78 BYRADIUS 1 KM WITHCOORD
```

## Implementation

- **Backend**: SQLite R*Tree extension for spatial indexing
- **Storage**: Coordinates stored as (longitude, latitude) pairs
- **Precision**: IEEE 754 double precision
- **Index**: Automatic R*Tree index for efficient radius queries
- **Distance**: Haversine formula for great-circle distances
