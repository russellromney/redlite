package com.redlite;

/**
 * Information about a key returned by KEYINFO command.
 *
 * @param keyType   The type of the key ("string", "hash", "list", "set", "zset", "json")
 * @param ttl       Time to live in seconds (-1 if no TTL, -2 if key doesn't exist)
 * @param createdAt Unix timestamp in milliseconds when the key was created
 * @param updatedAt Unix timestamp in milliseconds when the key was last updated
 */
public record KeyInfo(String keyType, long ttl, long createdAt, long updatedAt) {

    /**
     * Check if this key has a TTL set.
     */
    public boolean hasTtl() {
        return ttl >= 0;
    }
}
