package com.github.cym2018.spark.udf.lib.utils;

public class ComparableUtil {
    private ComparableUtil(){}
    public static <T extends Comparable<T>> T max(T a, T b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.compareTo(b) > 0 ? a : b;
    }
    public static <T extends Comparable<T>> T min(T a, T b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.compareTo(b) < 0 ? a : b;
    }
}
