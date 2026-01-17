package com.redlite.namespaces

import com.redlite.GeoMember
import com.redlite.Redlite

/**
 * Geospatial namespace for GEO* commands.
 */
class GeoNamespace(private val client: Redlite) {

    /**
     * Add geospatial items.
     *
     * @param key Geo key
     * @param members Geospatial members with coordinates
     */
    fun add(key: String, vararg members: GeoMember): Long {
        if (members.isEmpty()) return 0
        val args = mutableListOf<Any>("GEOADD", key)
        members.forEach { member ->
            args.addAll(listOf(
                member.longitude.toString(),
                member.latitude.toString(),
                member.member
            ))
        }
        val result = client.execute(*args.toTypedArray())
        return (result as? Number)?.toLong() ?: 0
    }

    /**
     * Search for members within radius.
     *
     * @param key Geo key
     * @param longitude Center longitude
     * @param latitude Center latitude
     * @param radius Search radius
     * @param unit Distance unit (m, km, mi, ft)
     * @param count Limit results
     * @param withdist Include distances
     * @param withcoord Include coordinates
     */
    fun search(
        key: String,
        longitude: Double,
        latitude: Double,
        radius: Double,
        unit: String = "km",
        count: Int? = null,
        withdist: Boolean = false,
        withcoord: Boolean = false
    ): List<Any> {
        val args = mutableListOf<Any>(
            "GEOSEARCH", key,
            "FROMLONLAT", longitude.toString(), latitude.toString(),
            "BYRADIUS", radius.toString(), unit.uppercase()
        )
        count?.let { args.addAll(listOf("COUNT", it.toString())) }
        if (withdist) args.add("WITHDIST")
        if (withcoord) args.add("WITHCOORD")
        return client.execute(*args.toTypedArray()) as? List<Any> ?: emptyList()
    }

    /**
     * Get distance between two members.
     *
     * @param key Geo key
     * @param member1 First member
     * @param member2 Second member
     * @param unit Distance unit (m, km, mi, ft)
     */
    fun dist(key: String, member1: String, member2: String, unit: String = "m"): Double? {
        val result = client.execute("GEODIST", key, member1, member2, unit.uppercase())
        return (result as? Number)?.toDouble()
    }

    /**
     * Get positions of members.
     */
    @Suppress("UNCHECKED_CAST")
    fun pos(key: String, vararg members: String): List<Pair<Double, Double>?> {
        if (members.isEmpty()) return emptyList()
        val args = mutableListOf<Any>("GEOPOS", key)
        args.addAll(members.toList())
        val result = client.execute(*args.toTypedArray()) as? List<*> ?: return emptyList()
        return result.map { item ->
            if (item is List<*> && item.size >= 2) {
                val lon = (item[0] as? Number)?.toDouble() ?: return@map null
                val lat = (item[1] as? Number)?.toDouble() ?: return@map null
                Pair(lon, lat)
            } else null
        }
    }

    /**
     * Get geohash of members.
     */
    fun hash(key: String, vararg members: String): List<String?> {
        if (members.isEmpty()) return emptyList()
        val args = mutableListOf<Any>("GEOHASH", key)
        args.addAll(members.toList())
        val result = client.execute(*args.toTypedArray()) as? List<*> ?: return emptyList()
        return result.map { it?.toString() }
    }

    /**
     * Search by member name.
     */
    fun searchByMember(
        key: String,
        member: String,
        radius: Double,
        unit: String = "km",
        count: Int? = null,
        withdist: Boolean = false,
        withcoord: Boolean = false
    ): List<Any> {
        val args = mutableListOf<Any>(
            "GEOSEARCH", key,
            "FROMMEMBER", member,
            "BYRADIUS", radius.toString(), unit.uppercase()
        )
        count?.let { args.addAll(listOf("COUNT", it.toString())) }
        if (withdist) args.add("WITHDIST")
        if (withcoord) args.add("WITHCOORD")
        return client.execute(*args.toTypedArray()) as? List<Any> ?: emptyList()
    }
}
