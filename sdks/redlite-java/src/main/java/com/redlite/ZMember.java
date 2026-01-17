package com.redlite;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Sorted set member with score.
 */
public record ZMember(double score, byte[] member) {

    /**
     * Create a ZMember from a string.
     */
    public static ZMember of(double score, String member) {
        return new ZMember(score, member.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Create a ZMember from bytes.
     */
    public static ZMember of(double score, byte[] member) {
        return new ZMember(score, member);
    }

    /**
     * Get the member as a string.
     */
    public String memberAsString() {
        return new String(member, StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZMember zMember = (ZMember) o;
        return Double.compare(zMember.score, score) == 0 &&
                Arrays.equals(member, zMember.member);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(score);
        result = 31 * result + Arrays.hashCode(member);
        return result;
    }

    @Override
    public String toString() {
        return "ZMember{score=" + score + ", member=" + memberAsString() + "}";
    }
}
