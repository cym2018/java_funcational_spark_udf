package com.github.cym2018.spark.udf.lib.calculators;

import com.github.cym2018.spark.udf.lib.interfaces.Calculator;
import com.github.cym2018.spark.udf.lib.utils.NumberUtil;
import org.apache.spark.sql.Row;

import javax.validation.constraints.NotNull;
import java.util.Map;

/**
 * sample1:
 * map1 = {"a": 1, "b": 2}
 * map2 = {"a": 3, "b": 4}
 * result: {"a": 4, "b": 6}
 * =====
 * sample2:
 * map1 = {"a":{"b": 1, "c": 2}, "b": {"b": 2, "c": 3}}
 * map2 = {"a":{"b": 3, "c": 4}, "b": {"b": 4, "c": 5}}
 * result: {"a":{"b": 4, "c": 6}, "b": {"b": 6, "c": 8}}
 */
@SuppressWarnings("unchecked")
public class MergeNumberMap<T> implements Calculator<T> {

    private final Calculator<T> provider;

    public MergeNumberMap(Calculator<T> provider) {
        this.provider = provider;
    }

    @Override
    public Object apply(Object preV, Row row) {
        return merge((T) preV, (T) provider.apply(preV, row));
    }

    private T merge(T currMap, T newMap) {
        if (currMap == null) {
            return newMap;
        }
        if (isNumberMap((Map<?, ?>) newMap)) {
            return (T) mergeNumberMap((Map<Object, Object>) currMap, (Map<Object, Object>) newMap);
        } else {
            return (T) mergeMap((Map<Object, Object>) currMap, (Map<Object, Object>) newMap);
        }
    }

    private Map<Object, Object> mergeMap(@NotNull Map<Object, Object> currMap, @NotNull Map<Object, Object> newMap) {
        // skip empty newMap
        if (newMap.isEmpty()) {
            return currMap;
        }
        // check if V is Number
        if (newMap.values().stream().findAny().get() instanceof Number) {
            // merge number map
            return mergeNumberMap(currMap, newMap);
        } else {
            // merge map
            for (Map.Entry<Object, Object> entry : newMap.entrySet()) {
                Object currValue = currMap.get(entry.getKey());
                if (currValue == null) {
                    currMap.put(entry.getKey(), entry.getValue());
                } else {
                    currMap.put(entry.getKey(), mergeMap((Map<Object, Object>) currValue, (Map<Object, Object>) entry.getValue()));
                }
            }
            return currMap;
        }
    }


    private Map<Object, Object> mergeNumberMap(@NotNull Map<Object, Object> currMap, @NotNull Map<Object, Object> newMap) {
        for (Map.Entry<?, ?> entry : newMap.entrySet()) {
            Object key = entry.getKey();
            Object newValue = entry.getValue();
            if (!(newValue instanceof Number)) {
                throw new IllegalArgumentException("Value in newMap must be a Number");
            }
            Object currValue = currMap.get(key);
            if (currValue == null) {
                currMap.put(key, newValue);
            } else {
                if (!(currValue instanceof Number)) {
                    throw new IllegalArgumentException("Value in currMap must be a Number");
                }
                currMap.put(key, NumberUtil.add((Number) currValue, (Number) newValue));
            }
        }
        return currMap;

    }



    private boolean isNumberMap(@NotNull Map<?, ?> map) {
        if (map.isEmpty()) {
            throw new IllegalArgumentException("Map is empty");
        }
        return map.values().stream().findAny().get() instanceof Number;
    }

    @Override
    public String toString() {
        return "MergeNumberMap(" + provider + ")";
    }
}
