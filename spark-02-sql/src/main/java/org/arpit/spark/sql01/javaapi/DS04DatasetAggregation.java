package org.arpit.spark.sql01.javaapi;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.arpit.spark.common.util.LoggerUtil;

import static org.apache.spark.sql.functions.*;

/**
 * Run {@link org.arpit.spark.sql00.common.EmployeeCsvWriter} to produce employee.csv file in resources folder
 */
public class DS04DatasetAggregation {

    private static final String APP_NAME = DS04DatasetAggregation.class.getName();

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

        Dataset<Row> employeesByJobTitle = dataset.groupBy(col("jobTitle")).count()
                .orderBy(col("count").desc());
        employeesByJobTitle.show(5, false);

        Dataset<Row> employeesByYoj = dataset
                .select(col("firstName"),
                        date_format(col("doj").divide(1000).cast(DataTypes.TimestampType), "yyyy").alias("yoj"),
                        col("city"))
                .orderBy(col("yoj"));
        //employeesByYoj.show(5);

        employeesByYoj = employeesByYoj
                .groupBy(col("yoj")).count()
                .orderBy(col("count").desc());
        employeesByYoj.show(5);

        Dataset<Row> averageSalaryByCity = dataset
                .groupBy(col("city")).agg(avg(col("salary")).alias("avg_salary"))
                .orderBy(col("avg_salary").desc());
        averageSalaryByCity.show(5);
    }
}
