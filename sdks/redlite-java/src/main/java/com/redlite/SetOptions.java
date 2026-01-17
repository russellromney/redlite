package com.redlite;

import org.jetbrains.annotations.Nullable;

/**
 * Options for SET command.
 */
public class SetOptions {
    private final @Nullable Long ex;
    private final @Nullable Long px;
    private final boolean nx;
    private final boolean xx;

    private SetOptions(@Nullable Long ex, @Nullable Long px, boolean nx, boolean xx) {
        this.ex = ex;
        this.px = px;
        this.nx = nx;
        this.xx = xx;
    }

    /**
     * Get expiration time in seconds.
     */
    public @Nullable Long getEx() {
        return ex;
    }

    /**
     * Get expiration time in milliseconds.
     */
    public @Nullable Long getPx() {
        return px;
    }

    /**
     * Only set if key does not exist.
     */
    public boolean isNx() {
        return nx;
    }

    /**
     * Only set if key exists.
     */
    public boolean isXx() {
        return xx;
    }

    /**
     * Create a new builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for SetOptions.
     */
    public static class Builder {
        private Long ex = null;
        private Long px = null;
        private boolean nx = false;
        private boolean xx = false;

        /**
         * Set expiration time in seconds.
         */
        public Builder ex(long seconds) {
            this.ex = seconds;
            return this;
        }

        /**
         * Set expiration time in milliseconds.
         */
        public Builder px(long milliseconds) {
            this.px = milliseconds;
            return this;
        }

        /**
         * Only set if key does not exist.
         */
        public Builder nx() {
            this.nx = true;
            return this;
        }

        /**
         * Only set if key exists.
         */
        public Builder xx() {
            this.xx = true;
            return this;
        }

        /**
         * Build the SetOptions.
         */
        public SetOptions build() {
            return new SetOptions(ex, px, nx, xx);
        }
    }
}
