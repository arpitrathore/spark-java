package org.arpit.spark.rdd01.basic;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Rdd02MapAndCollect {

    private static final String APP_NAME = Rdd02MapAndCollect.class.getName();

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.OFF);

        List<Integer> inputData = Arrays.asList(25, 43, 20, 77, 88, 99);
        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(inputData);

        JavaRDD<Double> resultRDD = rdd.map(i -> Math.sqrt(i));

        long startTime = System.currentTimeMillis();
        System.out.println(resultRDD.collect());
        System.out.println("Collect time taken : " + (System.currentTimeMillis() - startTime) + " ms");
        sc.close();
    }
}
