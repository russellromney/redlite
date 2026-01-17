package com.redlite;

import org.jetbrains.annotations.Nullable;

/**
 * Options for SCAN commands.
 */
public class ScanOptions {
    private final @Nullable String match;
    private final int count;

    private ScanOptions(@Nullable String match, int count) {
        this.match = match;
        this.count = count;
    }

    /**
     * Get the match pattern.
     */
    public @Nullable String getMatch() {
        return match;
    }

    /**
     * Get the count hint.
     */
    public int getCount() {
        return count;
    }

    /**
     * Create a new builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for ScanOptions.
     */
    public static class Builder {
        private String match = null;
        private int count = 10;

        /**
         * Set the match pattern.
         */
        public Builder match(String pattern) {
            this.match = pattern;
            return this;
        }

        /**
         * Set the count hint.
         */
        public Builder count(int count) {
            this.count = count;
            return this;
        }

        /**
         * Build the ScanOptions.
         */
        public ScanOptions build() {
            return new ScanOptions(match, count);
        }
    }
}
