package com.redlite;

/**
 * Geospatial member with coordinates.
 */
public record GeoMember(double longitude, double latitude, String member) {

    /**
     * Create a GeoMember.
     */
    public static GeoMember of(double longitude, double latitude, String member) {
        return new GeoMember(longitude, latitude, member);
    }
}
