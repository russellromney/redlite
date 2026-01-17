package com.redlite

/**
 * Options for SET command.
 *
 * @property ex Expiration time in seconds
 * @property px Expiration time in milliseconds
 * @property nx Only set if key does not exist
 * @property xx Only set if key exists
 */
data class SetOptions(
    val ex: Long? = null,
    val px: Long? = null,
    val nx: Boolean = false,
    val xx: Boolean = false
) {
    /**
     * Builder pattern for Java interoperability.
     */
    class Builder {
        private var ex: Long? = null
        private var px: Long? = null
        private var nx: Boolean = false
        private var xx: Boolean = false

        fun ex(seconds: Long) = apply { this.ex = seconds }
        fun px(milliseconds: Long) = apply { this.px = milliseconds }
        fun nx() = apply { this.nx = true }
        fun xx() = apply { this.xx = true }
        fun build() = SetOptions(ex, px, nx, xx)
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }
}
