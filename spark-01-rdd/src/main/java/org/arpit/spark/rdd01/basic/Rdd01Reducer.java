package org.arpit.spark.rdd01.basic;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Rdd01Reducer {

    private static final String APP_NAME = Rdd01Reducer.class.getName();

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.OFF);

        List<Double> inputData = Arrays.asList(25.5, 43.45, 20.44, 77.77, 88.99, 99.33);
        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Double> rdd = sc.parallelize(inputData);

        long startTime = System.currentTimeMillis();
        Double result = rdd.reduce((v1, v2) -> v1 + v2);
        System.out.println(result);
        System.out.println("Reduce time taken : " + (System.currentTimeMillis() - startTime) + " ms");
        sc.close();
    }
}
