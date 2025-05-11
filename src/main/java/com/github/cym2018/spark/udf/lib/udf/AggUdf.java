package com.github.cym2018.spark.udf.lib.udf;

import com.github.cym2018.spark.udf.lib.interfaces.Calculator;
import com.github.cym2018.spark.udf.lib.utils.ScalaUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import scala.collection.mutable.WrappedArray;

import static com.github.cym2018.spark.udf.lib.Functions.*;

public class AggUdf {
    private AggUdf() {
    }

    public static Column call(Column... args) {
        UDF1<WrappedArray<Row>, Row> udf = rowArray -> {
            Calculator<Row> calculator = struct(
                    merge_number_map("pnl_map"),
                    build_map(col("paa"), merge_number_map("pnl_map")),
                    build_map(
                            col("position"),
                            struct(
                                    col("prod_code"),
                                    col("sub_prod_code"),
                                    col("region"),
                                    build_map(
                                            col("fo_mkf"),
                                            struct(
                                                    col("mapped_mkf"),
                                                    col("exposure_type"),
                                                    merge_number_map("pnl_map")
                                            )
                                    )
                            )
                    )
            );
            Object[] value = new Object[1];
            ScalaUtil.forEach(rowArray, row -> {
                value[0] = calculator.apply(value[0], row);
            });
            return calculator.getResult(value[0]);
        };
        return functions.udf(udf, DataType.fromDDL(
                "struct<" +
                        "  pnls:map<string, double>," +
                        "  paa_pnls:map<string, map<string,double>>," +
                        "  position_map:map<string, " +
                        "    struct<" +
                        "      prod_code:string," +
                        "      sub_prod_code:string," +
                        "      region:string," +
                        "      fo_mkf_pnls:map<string, " +
                        "        struct<" +
                        "          mapped_mkf:string," +
                        "          exposure_type:string," +
                        "          pnl_map:map<string, double>" +
                        "        >" +
                        "      >" +
                        "    >" +
                        "  >" +
                        ">"
        )).apply(args);
    }
}
