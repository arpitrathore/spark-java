package org.arpit.spark.sql02.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.arpit.spark.common.util.LoggerUtil;

/**
 * Run {@link org.arpit.spark.sql00.common.EmployeeCsvWriter} to produce employee.csv file in resources folder
 */
public class DS01ShowAll {

    private static final String APP_NAME = DS01ShowAll.class.getName();

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

        final Dataset<Row> employeesByCity = spark.sql("select city, count(1) as city_count from employee" +
                " group by city order by city_count desc");
        employeesByCity.printSchema();
        employeesByCity.show(5);

        final Dataset<Row> averageSalaryByCity = spark.sql("select city, avg(salary) as avg_sal from employee" +
                " group by city order by avg_sal desc");
        averageSalaryByCity.show(5);

        final Dataset<Row> averageSalaryByCityAndJobTitle = spark.sql("select city, jobTitle, avg(salary) as avg_sal from employee" +
                " group by city, jobTitle order by avg_sal desc limit 5");
        averageSalaryByCityAndJobTitle.show(10, false);

        averageSalaryByCityAndJobTitle.coalesce(1)
                .write()
                .option("header", true)
                .csv("spark-02-sql/src/main/resources/salary-by-city");
    }
}
