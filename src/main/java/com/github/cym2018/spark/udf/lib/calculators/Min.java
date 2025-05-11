package com.github.cym2018.spark.udf.lib.calculators;

import com.github.cym2018.spark.udf.lib.interfaces.Calculator;
import com.github.cym2018.spark.udf.lib.utils.ComparableUtil;
import org.apache.spark.sql.Row;

public class Min<T> implements Calculator<T> {
    private final Calculator<T> provider;

    public Min(Calculator<T> provider) {
        this.provider = provider;
    }

    @Override
    public Object apply(Object preV, Row row) {
        //noinspection unchecked
        T newV = (T) provider.apply(null, row);
        if (preV == null) {
            return newV;
        }
        if (preV instanceof Comparable) {
            //noinspection unchecked,rawtypes
            return (T) ComparableUtil.min((Comparable)preV,(Comparable)newV);
        }
        throw new IllegalArgumentException("preV is not comparable");
    }

    @Override
    public String toString() {
        return "Min(" + provider + ")";
    }
}
