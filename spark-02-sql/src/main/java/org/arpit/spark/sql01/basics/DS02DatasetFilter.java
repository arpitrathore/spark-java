package org.arpit.spark.sql01.basics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.functions;
import org.arpit.spark.common.util.LoggerUtil;

/**
 * Run {@link org.arpit.spark.sql00.common.EmployeeCsvWriter} to produce employee.csv file in resources folder
 */
public class DS02DatasetFilter {

    private static final String APP_NAME = DS02DatasetFilter.class.getName();

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

        // Filter using string expression
        final Dataset<Row> youngFemaleEmployees = dataset.filter("gender='Female' AND age < 30");
        System.out.println("Count of young female employees : " + youngFemaleEmployees.count());
        youngFemaleEmployees.show(5);

        // Filter using lambda expression
        final Dataset<Row> gmailEmployees = dataset.filter((Row row) -> row.getAs("email").toString().endsWith("@gmail.com"));
        System.out.println("Count of gmail employees : " + gmailEmployees.count());
        gmailEmployees.show(5);

        // Filter using column expression
        final Dataset<Row> mumbaiManagerEmployees = dataset.filter(col("city").equalTo("Mumbai")
                .and(col("jobTitle").like("%Manager%")));
        System.out.println("Count of Mumbai managers employees : " + mumbaiManagerEmployees.count());
        mumbaiManagerEmployees.show(5);

    }
}
