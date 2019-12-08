package org.arpit.spark.rdd01.basic;

import com.google.common.collect.ImmutableList;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.arpit.spark.common.util.LoggerUtil;

import java.util.Arrays;
import java.util.List;

public class Rdd08FlatMap {

    private static final String APP_NAME = Rdd08FlatMap.class.getName();

    public static void main(String[] args) {
        LoggerUtil.disableSparkLogs();

        List<String> inputData = ImmutableList.of("Hi! My name is arpit",
                "",
                "I am from pune",
                "My favourite programming language is Java",
                "",
                "But I also like scala!!");

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<String> sentences = sc.parallelize(inputData);
        final JavaRDD<String> words = sentences.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        words.collect().forEach(System.out::println);

        sc.close();
    }

}


