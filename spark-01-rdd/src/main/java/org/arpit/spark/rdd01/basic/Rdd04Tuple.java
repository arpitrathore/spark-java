package org.arpit.spark.rdd01.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.arpit.spark.common.util.LoggerUtil;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Rdd04Tuple {

    private static final String APP_NAME = Rdd04Tuple.class.getName();

    public static void main(String[] args) {
        LoggerUtil.disableSparkLogs();

        List<Integer> inputData = Arrays.asList(25, 43, 20, 77, 88, 99, 144, 196, 65536);
        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> integerRDD = sc.parallelize(inputData);
        JavaRDD<Tuple2<Integer, Double>> intSqrtTuple = integerRDD.map(i -> new Tuple2<>(i, Math.sqrt(i)));

        System.out.println("<int, sqrt> tuple RDD : " + intSqrtTuple.collect());
        sc.close();
    }
}
