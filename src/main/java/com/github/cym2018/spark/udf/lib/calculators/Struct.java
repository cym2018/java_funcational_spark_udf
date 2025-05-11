package com.github.cym2018.spark.udf.lib.calculators;

import com.github.cym2018.spark.udf.lib.interfaces.Calculator;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

@SuppressWarnings("rawtypes")
public class Struct implements Calculator<Row> {

    private final Calculator[] providers;

    public Struct(Calculator[] providers) {
        this.providers = providers;
    }


    @Override
    public Object apply(Object preV, Row row) {
        Row preRow = (Row) preV;
        Object[] result = new Object[providers.length];
        for (int i = 0; i < providers.length; i++) {
            result[i] = providers[i].apply(preRow == null ? null : preRow.apply(i), row);
        }
        return RowFactory.create(result);
    }

    @Override
    public Row getResult(Object preV) {
        return (Row) preV;
    }
}
