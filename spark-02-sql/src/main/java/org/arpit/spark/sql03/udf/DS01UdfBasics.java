package org.arpit.spark.sql03.udf;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.arpit.spark.common.util.LoggerUtil;

import static org.apache.spark.sql.functions.*;


/**
 * Run {@link org.arpit.spark.sql00.common.EmployeeCsvWriter} to produce employee.csv file in resources folder
 */
public class DS01UdfBasics {

    private static final String APP_NAME = DS01UdfBasics.class.getName();

    public static void main(String[] args) {
        LoggerUtil.disableSparkLogs();

        SparkSession spark = SparkSession.builder()
                .appName(APP_NAME)
                .master("local[*]")
                .getOrCreate();

        spark.udf().register("isSenior", (String gender, Integer age) -> {
                    if (gender.equals("Female")) {
                        return age > 50;
                    } else {
                        return age > 55;
                    }
                },
                DataTypes.BooleanType);

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("spark-02-sql/src/main/resources/employees.csv");

        dataset.createOrReplaceTempView("employee");

        final Dataset<Row> employees = spark.sql("select id, firstName, age, gender from employee where age>45");
        employees.show(5);

        Dataset<Row> seniorCitizenEmployees = employees
                .withColumn("senior",
                        functions.callUDF("isSenior", col("gender"), col("age")));
        seniorCitizenEmployees.show(10);

        spark.sql("select age, gender, isSenior(gender, age) from employee").show(10);

        spark.sql("select gender, count(isSenior(gender, age)) as senior_count from employee group by gender").show(5);
    }
}
