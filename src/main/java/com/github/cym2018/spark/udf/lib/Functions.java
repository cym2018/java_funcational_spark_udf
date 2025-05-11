package com.github.cym2018.spark.udf.lib;

import com.github.cym2018.spark.udf.lib.calculators.*;
import com.github.cym2018.spark.udf.lib.interfaces.Calculator;
import org.apache.spark.sql.Row;

import java.util.Map;

public class Functions {

    public static <T> Calculator<T> col(String name) {
        return new Column<>(name);
    }

    public static <T> Calculator<T> lit(T value) {
        return new Literal<>(value);
    }

    public static <T> Calculator<T> merge_number_map(Calculator<T> map_provider) {
        return new MergeNumberMap<>(map_provider);
    }

    public static <T> Calculator<T> merge_number_map(String map_name) {
        return new MergeNumberMap<>(col(map_name));
    }
    public static <K,V> Calculator<Map<K,V>> build_map(Calculator<K> key, Calculator<V> val) {
        return new MapBuilder<>(key, val);
    }

    @SuppressWarnings("rawtypes")
    public static Calculator<Row> struct(Calculator... providers) {
        return new Struct(providers);
    }
}
