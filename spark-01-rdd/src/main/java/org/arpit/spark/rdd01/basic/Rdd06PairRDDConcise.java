package org.arpit.spark.rdd01.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.arpit.spark.common.util.Employee;
import org.arpit.spark.common.util.LoggerUtil;
import scala.Tuple2;

import java.util.List;

public class Rdd06PairRDDConcise {

    private static final String APP_NAME = Rdd06PairRDDConcise.class.getName();

    public static void main(String[] args) {
        LoggerUtil.disableSparkLogs();

        List<Employee> inputData = Employee.buildRandomEmployees(50);

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println("Unsorted employees groupBy country using method chaining -");

        sc.parallelize(inputData)
                .mapToPair(e -> new Tuple2<>(e.getCountry(), 1L))
                .reduceByKey((v1, v2) -> v1 + v2)
                .collect()
                .forEach(System.out::println);

        sc.close();
    }

}


