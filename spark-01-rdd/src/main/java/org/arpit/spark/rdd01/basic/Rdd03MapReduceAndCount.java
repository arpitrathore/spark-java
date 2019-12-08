package org.arpit.spark.rdd01.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.arpit.spark.common.util.LoggerUtil;

import java.util.Arrays;
import java.util.List;

public class Rdd03MapReduceAndCount {

    private static final String APP_NAME = Rdd03MapReduceAndCount.class.getName();

    public static void main(String[] args) {
        LoggerUtil.disableSparkLogs();

        List<Integer> inputData = Arrays.asList(25, 43, 20, 77, 88, 99);
        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(inputData);

        long startTime = System.currentTimeMillis();
        Long count = rdd.map(i -> 1L).reduce((v1, v2) -> v1 + v2);
        System.out.println("RDD count : " + count);
        System.out.println("RDD count with rdd.count() method " + rdd.count());
        System.out.println("Map reduce time taken : " + (System.currentTimeMillis() - startTime) + " ms");
        sc.close();
    }
}
