package com.github.cym2018.spark.udf.lib.calculators;

import com.github.cym2018.spark.udf.lib.interfaces.Calculator;
import org.apache.spark.sql.Row;


public class Literal<T> implements Calculator<T> {
    final T finalValue;

    public Literal(T value) {
        this.finalValue = value;
    }

    @Override
    public Object apply(Object preV, Row row) {
        return finalValue;
    }

    @Override
    public T getResult(Object preV) {
        return finalValue;
    }

    @Override
    public String toString() {
        return "Literal(" + finalValue + ")";
    }
}
