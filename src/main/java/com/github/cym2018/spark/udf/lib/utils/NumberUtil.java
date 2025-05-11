package com.github.cym2018.spark.udf.lib.utils;

import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.math.BigInteger;

public class NumberUtil {
    private NumberUtil() {
    }

    public static Number add(@NotNull Number a, @NotNull Number b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        if (a instanceof Integer) {
            return ((Integer) a) + ((Integer) b);
        }
        if (a instanceof Long) {
            return ((Long) a) + ((Long) b);
        }
        if (a instanceof Float) {
            return ((Float) a) + ((Float) b);
        }
        if (a instanceof Double) {
            return ((Double) a) + ((Double) b);
        }
        if (a instanceof Short) {
            return (short) (((Short) a) + ((Short) b));
        }
        if (a instanceof Byte) {
            return (byte) (((Byte) a) + ((Byte) b));
        }
        if (a instanceof BigDecimal) {
            return ((BigDecimal) a).add((BigDecimal) b);
        }
        if (a instanceof BigInteger) {
            return ((BigInteger) a).add((BigInteger) b);
        }
        throw new IllegalArgumentException("Unsupported number type: " + a.getClass().getName());
    }
}
