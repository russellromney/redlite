package com.redlite;

/**
 * Exception thrown by Redlite operations.
 */
public class RedliteException extends RuntimeException {

    public RedliteException(String message) {
        super(message);
    }

    public RedliteException(String message, Throwable cause) {
        super(message, cause);
    }
}
