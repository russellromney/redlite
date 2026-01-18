package com.redlite

/**
 * Options for JSON.SET command.
 */
class JsonSetOptions private constructor(
    val nx: Boolean = false,
    val xx: Boolean = false
) {
    companion object {
        /**
         * Only set if key does not exist (NX flag).
         */
        fun onlyIfNotExists(): JsonSetOptions = JsonSetOptions(nx = true)

        /**
         * Only set if key exists (XX flag).
         */
        fun onlyIfExists(): JsonSetOptions = JsonSetOptions(xx = true)
    }
}
