package org.arpit.spark.sql01.javaapi;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.arpit.spark.common.util.LoggerUtil;

/**
 * Run {@link org.arpit.spark.sql00.common.EmployeeCsvWriter} to produce employee.csv file in resources folder
 */
public class DS01ReadCsvWriteOrc {

    private static final String APP_NAME = DS01ReadCsvWriteOrc.class.getName();

    public static void main(String[] args) {
        LoggerUtil.disableSparkLogs();

        SparkSession spark = SparkSession.builder()
                .appName(APP_NAME)
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("spark-02-sql/src/main/resources/employees.csv");

        dataset.printSchema();
        dataset.show();

        final String hdfsOutputPath = "hdfs://localhost:9000/data/DS01ReadCsvWriteOrc";
        dataset.coalesce(1).write().format("orc").mode("overwrite").save(hdfsOutputPath);
        System.out.println("Data written in ORC format at HDFS location : " + hdfsOutputPath);
    }
}
