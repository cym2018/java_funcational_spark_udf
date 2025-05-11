package com.github.cym2018.spark.udf.lib.utils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.ArrayList;
import java.util.List;

public class SparkUtil {
    private SparkUtil() {
    }

    public static List<Row> toRows(Object[][] objs) {
        List<Row> rows = new ArrayList<>();
        for (Object[] obj : objs) {
            rows.add(RowFactory.create(obj));
        }
        return rows;

    }
}
