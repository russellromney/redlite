package com.redlite;

import java.util.List;

/**
 * Result of a SCAN operation.
 *
 * @param <T> The type of elements in the result
 */
public class ScanResult<T> {
    private final String cursor;
    private final List<T> results;

    public ScanResult(String cursor, List<T> results) {
        this.cursor = cursor;
        this.results = results;
    }

    /**
     * Get the cursor for the next iteration.
     * "0" indicates the scan is complete.
     */
    public String getCursor() {
        return cursor;
    }

    /**
     * Get the results from this iteration.
     */
    public List<T> getResults() {
        return results;
    }

    /**
     * Check if the scan is complete.
     */
    public boolean isComplete() {
        return "0".equals(cursor);
    }
}
