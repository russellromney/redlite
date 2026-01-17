package com.redlite

/**
 * Sorted set member with score.
 *
 * @property score The score associated with the member
 * @property member The member value as bytes
 */
data class ZMember(
    val score: Double,
    val member: ByteArray
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as ZMember
        if (score != other.score) return false
        if (!member.contentEquals(other.member)) return false
        return true
    }

    override fun hashCode(): Int {
        var result = score.hashCode()
        result = 31 * result + member.contentHashCode()
        return result
    }

    override fun toString(): String {
        return "ZMember(score=$score, member=${member.decodeToString()})"
    }

    companion object {
        /**
         * Create a ZMember from a string member.
         */
        @JvmStatic
        fun of(score: Double, member: String) = ZMember(score, member.toByteArray())
    }
}
