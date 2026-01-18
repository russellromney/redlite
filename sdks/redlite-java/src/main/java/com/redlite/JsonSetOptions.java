package com.redlite;

/**
 * Options for JSON.SET command.
 */
public class JsonSetOptions {
    private boolean nx = false;
    private boolean xx = false;

    /**
     * Only set if key does not exist (NX flag).
     */
    public boolean isNx() {
        return nx;
    }

    /**
     * Only set if key does not exist (NX flag).
     */
    public JsonSetOptions nx() {
        this.nx = true;
        this.xx = false;
        return this;
    }

    /**
     * Only set if key exists (XX flag).
     */
    public boolean isXx() {
        return xx;
    }

    /**
     * Only set if key exists (XX flag).
     */
    public JsonSetOptions xx() {
        this.xx = true;
        this.nx = false;
        return this;
    }

    /**
     * Create options with NX flag.
     */
    public static JsonSetOptions onlyIfNotExists() {
        return new JsonSetOptions().nx();
    }

    /**
     * Create options with XX flag.
     */
    public static JsonSetOptions onlyIfExists() {
        return new JsonSetOptions().xx();
    }
}
