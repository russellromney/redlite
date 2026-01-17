package com.redlite.namespaces

import com.redlite.Redlite

/**
 * Vector search namespace for Redlite V* commands.
 */
class VectorNamespace(private val client: Redlite) {

    /**
     * Add a vector to a vector set.
     *
     * @param key Vector set key
     * @param element Element identifier
     * @param vector Vector values
     * @param attributes Optional JSON attributes
     */
    fun add(
        key: String,
        element: String,
        vector: List<Double>,
        attributes: Map<String, Any>? = null
    ): Boolean {
        val args = mutableListOf<Any>("VADD", key, "VALUES", vector.size.toString())
        args.addAll(vector.map { it.toString() })
        args.add(element)
        attributes?.let {
            val json = buildJsonString(it)
            args.addAll(listOf("SETATTR", json))
        }
        client.execute(*args.toTypedArray())
        return true
    }

    /**
     * Find similar vectors.
     *
     * @param key Vector set key
     * @param vector Query vector
     * @param count Number of results
     * @param withscores Include distance scores
     */
    fun sim(
        key: String,
        vector: List<Double>,
        count: Int = 10,
        withscores: Boolean = false
    ): List<Any> {
        val args = mutableListOf<Any>("VSIM", key, "VALUES", vector.size.toString())
        args.addAll(vector.map { it.toString() })
        args.addAll(listOf("COUNT", count.toString()))
        if (withscores) args.add("WITHSCORES")
        return client.execute(*args.toTypedArray()) as? List<Any> ?: emptyList()
    }

    /**
     * Remove element from vector set.
     */
    fun rem(key: String, element: String): Long {
        val result = client.execute("VREM", key, element)
        return (result as? Number)?.toLong() ?: 0
    }

    /**
     * Get number of elements in vector set.
     */
    fun card(key: String): Long {
        val result = client.execute("VCARD", key)
        return (result as? Number)?.toLong() ?: 0
    }

    /**
     * Get vector dimension.
     */
    fun dim(key: String): Long {
        val result = client.execute("VDIM", key)
        return (result as? Number)?.toLong() ?: 0
    }

    /**
     * Get vector set information.
     */
    @Suppress("UNCHECKED_CAST")
    fun info(key: String): Map<String, Any> {
        val result = client.execute("VINFO", key)
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

    private fun buildJsonString(map: Map<String, Any>): String {
        val sb = StringBuilder("{")
        map.entries.forEachIndexed { index, (k, v) ->
            if (index > 0) sb.append(",")
            sb.append("\"").append(k).append("\":")
            when (v) {
                is String -> sb.append("\"").append(v).append("\"")
                is Number -> sb.append(v)
                is Boolean -> sb.append(v)
                else -> sb.append("\"").append(v.toString()).append("\"")
            }
        }
        sb.append("}")
        return sb.toString()
    }
}
