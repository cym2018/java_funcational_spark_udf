package com.github.cym2018.spark.udf.lib.calculators;

import com.github.cym2018.spark.udf.lib.interfaces.Calculator;
import com.github.cym2018.spark.udf.lib.utils.ScalaUtil;
import org.apache.spark.sql.Row;

public class Column<T> implements Calculator<T> {
    private final String name;

    public Column(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Column(" + name + ")";
    }

    @Override
    public Object apply(Object preV, Row row) {
        if (row.getAs(name) instanceof scala.collection.Map) {
            return ScalaUtil.newJavaMap((scala.collection.Map<?,?>) row.getAs(name));
        }
        return row.getAs(name);
    }
}
