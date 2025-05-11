package com.github.cym2018.spark.udf.lib.calculators;

import com.github.cym2018.spark.udf.lib.interfaces.Calculator;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

public class MapBuilder<K, V> implements Calculator<Map<K, V>> {
    private final Calculator<K> keyCalculator;
    private final Calculator<V> valueCalculator;

    public MapBuilder(Calculator<K> keyCalculator, Calculator<V> valueCalculator) {
        this.keyCalculator = keyCalculator;
        this.valueCalculator = valueCalculator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object apply(Object preMap, Row row) {
        if (preMap == null) {
            preMap = new HashMap<>();
        }
        K key = (K) keyCalculator.apply(null, row);
        ((Map<K, V>) preMap).compute(key, (k, preV) -> valueCalculator.applyAndGet(preV, row));
        return preMap;
    }
}
