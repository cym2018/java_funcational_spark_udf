package com.github.cym2018.spark.udf.lib.interfaces;

import org.apache.spark.sql.Row;

public interface Calculator<V> {
    Object apply(Object preV, Row row);

    @SuppressWarnings("unchecked")
    default V getResult(Object preV) {
        return (V) preV;
    }

    default V applyAndGet(Object preV, Row row) {
        return getResult(apply(preV, row));
    }
}
