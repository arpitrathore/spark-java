package org.arpit.spark.sql02.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.arpit.spark.common.util.LoggerUtil;

/**
 * Run {@link org.arpit.spark.sql00.common.EmployeeCsvWriter} to produce employee.csv file in resources folder
 */
public class DS02Aggregation {

    private static final String APP_NAME = DS02Aggregation.class.getName();

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

        dataset.createOrReplaceTempView("employee");

        final Dataset<Row> allEmployees = spark.sql("select * from employee limit 5");
        allEmployees.printSchema();
        allEmployees.show();
    }
}
