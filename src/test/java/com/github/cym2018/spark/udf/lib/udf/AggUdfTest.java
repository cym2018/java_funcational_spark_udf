package com.github.cym2018.spark.udf.lib.udf;

import com.github.cym2018.spark.udf.lib.utils.JavaUtil;
import com.github.cym2018.spark.udf.lib.utils.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

class AggUdfTest {
    @Test
    public void test123() {
        SparkSession spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate();
        spark.sql("select 1").show(10, false);
        Dataset<Row> ds = spark.createDataFrame(
                SparkUtil.toRows(new Object[][]{
                        {JavaUtil.toMap("a", 1d, "b", 6d), "paa_1", "position_1", "prod_code_1", "sub_prod_code_1", "region_1", "fo_mkf_1", "1.1", "L"},
                        {JavaUtil.toMap("a", 2d, "b", 7d), "paa_1", "position_1", "prod_code_1", "sub_prod_code_1", "region_1", "fo_mkf_2", "1.3", "L"},
                        {JavaUtil.toMap("a", 3d, "c", 8d), "paa_2", "position_1", "prod_code_1", "sub_prod_code_3", "region_1", "fo_mkf_3", "1.1", "L"},
                        {JavaUtil.toMap("a", 4d, "d", 9d), "paa_1", "position_2", "prod_code_1", "sub_prod_code_1", "region_1", "fo_mkf_4", "1.1", "L"},
                        {JavaUtil.toMap("a", 5d, "d", 10d), "paa_1", "position_3", "prod_code_1", "sub_prod_code_1", "region_1", "fo_mkf_5", "1.1", "1"},

                }),
                StructType.fromDDL("pnl_map map<string, double>, paa string, position string, prod_code string, sub_prod_code string, region string, fo_mkf string, mapped_mkf string, exposure_type string")

        );
        ds.groupBy().agg(AggUdf.call(functions.collect_list(functions.struct(
                                        "pnl_map",
                                        "paa",
                                        "position",
                                        "prod_code",
                                        "sub_prod_code",
                                        "region",
                                        "fo_mkf",
                                        "mapped_mkf",
                                        "exposure_type"))))
                .show(10, false);

    }

}