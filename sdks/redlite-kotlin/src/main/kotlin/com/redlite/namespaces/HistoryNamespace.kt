package com.redlite.namespaces

import com.redlite.Redlite

/**
 * Version history namespace for Redlite HISTORY.* commands.
 */
class HistoryNamespace(private val client: Redlite) {

    /**
     * Enable history tracking for a key pattern.
     *
     * @param pattern Key pattern (e.g., "user:*")
     * @param maxVersions Maximum versions to keep per key
     */
    fun enable(pattern: String = "*", maxVersions: Int = 100): Boolean {
        client.execute("HISTORY.ENABLE", pattern, maxVersions.toString())
        return true
    }

    /**
     * Disable history tracking for a key pattern.
     */
    fun disable(pattern: String = "*"): Boolean {
        client.execute("HISTORY.DISABLE", pattern)
        return true
    }

    /**
     * Get version history of a key.
     *
     * @param key The key
     * @param start Start version (0 = oldest)
     * @param end End version (-1 = newest)
     */
    fun get(key: String, start: Long = 0, end: Long = -1): List<HistoryEntry> {
        val result = client.execute("HISTORY.GET", key, start.toString(), end.toString())
        if (result is List<*>) {
            return result.mapNotNull { entry ->
                if (entry is List<*> && entry.size >= 3) {
                    HistoryEntry(
                        version = (entry[0] as? Number)?.toLong() ?: 0,
                        timestamp = (entry[1] as? Number)?.toLong() ?: 0,
                        value = entry[2] as? ByteArray
                    )
                } else null
            }
        }
        return emptyList()
    }

    /**
     * Get a specific version of a key.
     */
    fun getVersion(key: String, version: Long): ByteArray? {
        val result = client.execute("HISTORY.GETVERSION", key, version.toString())
        return result as? ByteArray
    }

    /**
     * Get the number of versions stored for a key.
     */
    fun count(key: String): Long {
        val result = client.execute("HISTORY.COUNT", key)
        return (result as? Number)?.toLong() ?: 0
    }

    /**
     * Revert a key to a previous version.
     */
    fun revert(key: String, version: Long): Boolean {
        client.execute("HISTORY.REVERT", key, version.toString())
        return true
    }

    /**
     * Trim old versions of a key.
     */
    fun trim(key: String, keepVersions: Int): Long {
        val result = client.execute("HISTORY.TRIM", key, keepVersions.toString())
        return (result as? Number)?.toLong() ?: 0
    }

    /**
     * Get history information for a key.
     */
    @Suppress("UNCHECKED_CAST")
    fun info(key: String): Map<String, Any> {
        val result = client.execute("HISTORY.INFO", key)
        if (result is List<*>) {
            val map = mutableMapOf<String, Any>()
            var i = 0
            while (i < result.size - 1) {
                val k = result[i]?.toString() ?: continue
                val v = result[i + 1] ?: continue
                map[k] = v
                i += 2
            }
            return map
        }
        return emptyMap()
    }
}

/**
 * A single history entry for a key.
 */
data class HistoryEntry(
    val version: Long,
    val timestamp: Long,
    val value: ByteArray?
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as HistoryEntry
        if (version != other.version) return false
        if (timestamp != other.timestamp) return false
        if (!value.contentEquals(other.value)) return false
        return true
    }

    override fun hashCode(): Int {
        var result = version.hashCode()
        result = 31 * result + timestamp.hashCode()
        result = 31 * result + (value?.contentHashCode() ?: 0)
        return result
    }
}
