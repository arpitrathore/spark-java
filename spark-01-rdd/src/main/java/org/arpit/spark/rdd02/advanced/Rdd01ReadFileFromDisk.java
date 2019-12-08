package org.arpit.spark.rdd02.advanced;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.arpit.spark.common.util.LoggerUtil;

import java.util.Arrays;

public class Rdd01ReadFileFromDisk {

    private static final String APP_NAME = Rdd01ReadFileFromDisk.class.getName();

    public static void main(String[] args) {
        LoggerUtil.disableSparkLogs();

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputData = sc.textFile("spark-01-rdd/src/main/resources/spark-logs.txt");

        final JavaRDD<String> filteredWords = inputData
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .filter(w -> !w.equals(""));

        filteredWords.collect().forEach(System.out::println);

        sc.close();
    }
}
