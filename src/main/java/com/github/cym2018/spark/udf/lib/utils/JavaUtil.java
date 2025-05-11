package com.github.cym2018.spark.udf.lib.utils;

import java.util.Map;

public class JavaUtil {
    private JavaUtil() {
    }

    public static Map<Object, Object> toMap(Object... args) {
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Arguments must be in pairs");
        }
        Map<Object, Object> map = new java.util.HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            map.put(args[i], args[i + 1]);
        }
        return map;
    }
}
