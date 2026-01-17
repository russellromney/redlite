package com.redlite

/**
 * Geospatial member with coordinates.
 *
 * @property longitude The longitude coordinate
 * @property latitude The latitude coordinate
 * @property member The member name
 */
data class GeoMember(
    val longitude: Double,
    val latitude: Double,
    val member: String
) {
    companion object {
        @JvmStatic
        fun of(longitude: Double, latitude: Double, member: String) =
            GeoMember(longitude, latitude, member)
    }
}
