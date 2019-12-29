package org.arpit.spark.sql01.javaapi;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.arpit.spark.common.util.LoggerUtil;

import static org.apache.spark.sql.functions.col;

/**
 * Run {@link org.arpit.spark.sql00.common.EmployeeCsvWriter} to produce employee.csv file in resources folder
 */
public class DS03DatasetSelect {

    private static final String APP_NAME = DS03DatasetSelect.class.getName();

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

        // Job role with highest salaries
        Dataset<Row> highestSalaryRoles = dataset
                .select(col("salary"), col("jobTitle"), col("city"))
                .orderBy(col("salary").desc());
        highestSalaryRoles.show(5, false);

        // Job role with lowest salaries
        Dataset<Row> lowestSalaryRoles = dataset
                .select(col("salary"), col("jobTitle"), col("city"))
                .orderBy(col("salary"));
        lowestSalaryRoles.show(5, false);


    }
}
