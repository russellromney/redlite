package com.redlite.namespaces

import com.redlite.Redlite
import com.redlite.RedliteException

/**
 * Full-text search namespace for Redlite FT.* commands.
 */
class FTSNamespace(private val client: Redlite) {

    // =========================================================================
    // FTS Enable/Disable Commands
    // =========================================================================

    /**
     * Enable FTS indexing globally.
     */
    fun enableGlobal() {
        throw RedliteException("FTS commands not yet implemented")
    }

    /**
     * Enable FTS indexing for a specific database.
     *
     * @param dbNum Database number
     */
    fun enableDatabase(dbNum: Int) {
        throw RedliteException("FTS commands not yet implemented")
    }

    /**
     * Enable FTS indexing for a key pattern.
     *
     * @param pattern Key pattern (e.g., "user:*")
     */
    fun enablePattern(pattern: String) {
        throw RedliteException("FTS commands not yet implemented")
    }

    /**
     * Enable FTS indexing for a specific key.
     *
     * @param key Key to enable FTS for
     */
    fun enableKey(key: String) {
        throw RedliteException("FTS commands not yet implemented")
    }

    /**
     * Disable FTS indexing globally.
     */
    fun disableGlobal() {
        throw RedliteException("FTS commands not yet implemented")
    }

    /**
     * Disable FTS indexing for a specific database.
     *
     * @param dbNum Database number
     */
    fun disableDatabase(dbNum: Int) {
        throw RedliteException("FTS commands not yet implemented")
    }

    /**
     * Disable FTS indexing for a key pattern.
     *
     * @param pattern Key pattern
     */
    fun disablePattern(pattern: String) {
        throw RedliteException("FTS commands not yet implemented")
    }

    /**
     * Disable FTS indexing for a specific key.
     *
     * @param key Key to disable FTS for
     */
    fun disableKey(key: String) {
        throw RedliteException("FTS commands not yet implemented")
    }

    /**
     * Check if FTS indexing is enabled for a key.
     *
     * @param key Key to check
     * @return true if FTS is enabled
     */
    fun isEnabled(key: String): Boolean {
        throw RedliteException("FTS commands not yet implemented")
    }

    // =========================================================================
    // FTS Search Commands
    // =========================================================================

    /**
     * Search an FTS index.
     *
     * @param index Index name
     * @param query Search query
     * @param nocontent Return only document IDs
     * @param limit Maximum results
     * @param offset Result offset
     * @param withscores Include BM25 scores
     * @return Search results
     */
    fun search(
        index: String,
        query: String,
        nocontent: Boolean = false,
        limit: Int = 10,
        offset: Int = 0,
        withscores: Boolean = false
    ): List<Any> {
        val args = mutableListOf<Any>("FT.SEARCH", index, query)
        if (nocontent) args.add("NOCONTENT")
        if (withscores) args.add("WITHSCORES")
        args.addAll(listOf("LIMIT", offset.toString(), limit.toString()))
        return client.execute(*args.toTypedArray()) as? List<Any> ?: emptyList()
    }

    /**
     * Create an FTS index.
     *
     * @param index Index name
     * @param schema Field definitions (field_name to field_type)
     * @param prefix Key prefix to index
     * @param on Data type (HASH or JSON)
     */
    fun create(
        index: String,
        schema: Map<String, String>,
        prefix: String? = null,
        on: String = "HASH"
    ): Boolean {
        val args = mutableListOf<Any>("FT.CREATE", index, "ON", on)
        prefix?.let { args.addAll(listOf("PREFIX", "1", it)) }
        args.add("SCHEMA")
        schema.forEach { (field, type) ->
            args.addAll(listOf(field, type))
        }
        client.execute(*args.toTypedArray())
        return true
    }

    /**
     * Drop an FTS index.
     *
     * @param index Index name
     * @param deleteDocs Also delete indexed documents
     */
    fun dropindex(index: String, deleteDocs: Boolean = false): Boolean {
        val args = mutableListOf<Any>("FT.DROPINDEX", index)
        if (deleteDocs) args.add("DD")
        client.execute(*args.toTypedArray())
        return true
    }

    /**
     * Get index information.
     */
    @Suppress("UNCHECKED_CAST")
    fun info(index: String): Map<String, Any> {
        val result = client.execute("FT.INFO", index)
        if (result is List<*>) {
            val map = mutableMapOf<String, Any>()
            var i = 0
            while (i < result.size - 1) {
                val key = result[i]?.toString() ?: continue
                val value = result[i + 1] ?: continue
                map[key] = value
                i += 2
            }
            return map
        }
        return emptyMap()
    }

    /**
     * Add a document to be indexed.
     */
    fun add(index: String, docId: String, score: Double = 1.0, fields: Map<String, String>): Boolean {
        val args = mutableListOf<Any>("FT.ADD", index, docId, score.toString())
        args.add("FIELDS")
        fields.forEach { (field, value) ->
            args.addAll(listOf(field, value))
        }
        client.execute(*args.toTypedArray())
        return true
    }

    /**
     * Delete a document from the index.
     */
    fun del(index: String, docId: String, deleteDocument: Boolean = false): Boolean {
        val args = mutableListOf<Any>("FT.DEL", index, docId)
        if (deleteDocument) args.add("DD")
        client.execute(*args.toTypedArray())
        return true
    }
}
