module github.com/russellromney/redlite/sdks/oracle/runners

go 1.21

require (
	github.com/russellromney/redlite/sdks/redlite-go v0.0.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/redis/go-redis/v9 v9.7.0 // indirect
)

replace github.com/russellromney/redlite/sdks/redlite-go => ../../redlite-go
