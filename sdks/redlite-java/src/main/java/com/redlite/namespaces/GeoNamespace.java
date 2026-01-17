package com.redlite.namespaces;

import com.redlite.GeoMember;
import com.redlite.Redlite;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * Geospatial namespace for GEO* commands.
 */
public class GeoNamespace {
    private final Redlite client;

    public GeoNamespace(Redlite client) {
        this.client = client;
    }

    /**
     * Add geospatial items.
     */
    public long add(String key, GeoMember... members) {
        // Would execute GEOADD command
        return 0;
    }

    /**
     * Search for members within radius.
     */
    public List<Object> search(String key, double longitude, double latitude,
                               double radius, String unit) {
        return search(key, longitude, latitude, radius, unit, null, false, false);
    }

    /**
     * Search for members within radius with options.
     */
    public List<Object> search(String key, double longitude, double latitude,
                               double radius, String unit, @Nullable Integer count,
                               boolean withdist, boolean withcoord) {
        // Would execute GEOSEARCH command
        return Collections.emptyList();
    }

    /**
     * Get distance between two members.
     */
    public @Nullable Double dist(String key, String member1, String member2, String unit) {
        // Would execute GEODIST command
        return null;
    }

    /**
     * Get positions of members.
     */
    public List<double[]> pos(String key, String... members) {
        // Would execute GEOPOS command
        return Collections.emptyList();
    }

    /**
     * Get geohash of members.
     */
    public List<String> hash(String key, String... members) {
        // Would execute GEOHASH command
        return Collections.emptyList();
    }
}
