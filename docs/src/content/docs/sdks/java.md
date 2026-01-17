---
title: Java/Kotlin SDK
description: Redlite SDK for Java and Kotlin (planned)
---

## Status

**Stub** - Not yet implemented.

Java and Kotlin SDKs are planned but not yet available. Oracle test runners exist for compatibility testing.

## Planned Features

- JNI bindings for native performance
- Kotlin coroutines support
- Android support

## Contributing

Want to help implement the Java/Kotlin SDK? See:

- [Java stub](https://github.com/russellromney/redlite/tree/main/sdks/redlite-java)
- [Kotlin stub](https://github.com/russellromney/redlite/tree/main/sdks/redlite-kotlin)
- [Oracle test runners](https://github.com/russellromney/redlite/tree/main/sdks/oracle/runners)

## Workaround

Use the server mode with [Jedis](https://github.com/redis/jedis) or [Lettuce](https://github.com/lettuce-io/lettuce-core):

```java
// Start redlite server
// redlite --server --port 6379

// Use Jedis
Jedis jedis = new Jedis("localhost", 6379);
jedis.set("key", "value");
String val = jedis.get("key");
```
