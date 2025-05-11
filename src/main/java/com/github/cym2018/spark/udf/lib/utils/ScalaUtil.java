package com.github.cym2018.spark.udf.lib.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ScalaUtil {
    private ScalaUtil() {
    }

    public static <K,V> Map<K,V> newJavaMap(scala.collection.Map<K,V> scalaMap) {
        if (scalaMap == null || scalaMap.isEmpty()) {
            return new HashMap<>();
        }
        if (scalaMap.head()._2() instanceof scala.collection.mutable.Map) {
            Map<Object, Object> result = new HashMap<>();
            forEach(scalaMap, (k, v) -> {
                //noinspection unchecked
                result.put(k, newJavaMap((scala.collection.Map<Object, Object>) v));
            });
            //noinspection unchecked
            return (Map<K, V>) result;
        }
        return new HashMap<>(scala.collection.JavaConverters.mapAsJavaMap(scalaMap));
    }

    public static <K, V> void forEach(scala.collection.Map<K, V> map, BiConsumer<K, V> consumer) {
        if (map == null || map.isEmpty()) {
            return;
        }
        forEach(map.toSeq(), a -> {
            try {
                consumer.accept(a._1(), a._2());
            } catch (Exception e) {
                throw new RuntimeException("Error in forEach " + a._1() + ", " + a._2(), e);
            }
        });
    }

    public static <A> void forEach(scala.collection.Seq<A> seq, Consumer<A> consumer) {
        if (seq == null || seq.isEmpty()) {
            return;
        }
        for (int i = 0; i < seq.length(); i++) {
            try {
                consumer.accept(seq.apply(i));
            } catch (Exception e) {
                throw new RuntimeException("Error in forEach " + i, e);
            }
        }
    }
}
