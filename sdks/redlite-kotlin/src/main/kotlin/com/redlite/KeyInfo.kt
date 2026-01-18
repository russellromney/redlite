package com.redlite

/**
 * Information about a key returned by KEYINFO command.
 *
 * @property keyType The type of the key ("string", "hash", "list", "set", "zset", "json")
 * @property ttl Time to live in seconds (-1 if no TTL, -2 if key doesn't exist)
 * @property createdAt Unix timestamp in milliseconds when the key was created
 * @property updatedAt Unix timestamp in milliseconds when the key was last updated
 */
data class KeyInfo(
    val keyType: String,
    val ttl: Long,
    val createdAt: Long,
    val updatedAt: Long
) {
    /**
     * Check if this key has a TTL set.
     */
    fun hasTtl(): Boolean = ttl >= 0
}
