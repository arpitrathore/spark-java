package org.arpit.spark.rdd01.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.arpit.spark.common.util.Employee;
import org.arpit.spark.common.util.LoggerUtil;
import scala.Tuple2;

import java.util.List;

public class Rdd05PairRDDReduceByKey {

    private static final String APP_NAME = Rdd05PairRDDReduceByKey.class.getName();

    public static void main(String[] args) {
        LoggerUtil.disableSparkLogs();

        List<Employee> inputData = Employee.buildRandomEmployees(50);

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Employee> employeeRDD = sc.parallelize(inputData);

        final JavaPairRDD<String, Long> countryEmployeePairRDD =
                employeeRDD.mapToPair(e -> new Tuple2<>(e.getCountry(), 1L));

        final JavaPairRDD<String, Long> countryCount = countryEmployeePairRDD
                .reduceByKey((v1, v2) -> v1 + v2);
        final List<Tuple2<String, Long>> collect = countryCount.collect();
        System.out.println("Unsorted employees groupBy country : " + collect);

        sc.close();
    }

}


