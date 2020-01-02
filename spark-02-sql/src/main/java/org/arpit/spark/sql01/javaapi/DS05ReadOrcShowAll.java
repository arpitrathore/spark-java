package org.arpit.spark.sql01.javaapi;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.arpit.spark.common.util.LoggerUtil;

/**
 * Run {@link DS01ReadCsvWriteOrc} to produce orc file in hdfs in /data/DS01ReadCsvWriteOrc directory
 * Run this to verify : $ hdfs dfs -ls -h /data/DS01ReadCsvWriteOrc
 */
public class DS05ReadOrcShowAll {

    private static final String APP_NAME = DS05ReadOrcShowAll.class.getName();

    // Run this to get full file path : $ hdfs dfs -ls -h /data/DS01ReadCsvWriteOrc
    private static final String ORC_FILE_PATH_HDFS = "hdfs://localhost:9000/data/DS01ReadCsvWriteOrc/part-00000-80863e6b-71d0-41ad-8aaa-d5e2a62e4fb5-c000.snappy.orc";

    public static void main(String[] args) {
        LoggerUtil.disableSparkLogs();

        SparkSession spark = SparkSession.builder()
                .appName(APP_NAME)
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .orc(ORC_FILE_PATH_HDFS);

        dataset.printSchema();
        dataset.show();
    }
}
