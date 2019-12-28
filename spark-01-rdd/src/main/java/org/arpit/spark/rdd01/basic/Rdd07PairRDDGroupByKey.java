package org.arpit.spark.rdd01.basic;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.arpit.spark.common.pojo.Employee;
import org.arpit.spark.common.util.EmployeeUtil;
import org.arpit.spark.common.util.LoggerUtil;
import scala.Tuple2;

import java.util.List;

public class Rdd07PairRDDGroupByKey {

    private static final String APP_NAME = Rdd07PairRDDGroupByKey.class.getName();

    public static void main(String[] args) {
        LoggerUtil.disableSparkLogs();

        List<Employee> inputData = EmployeeUtil.buildRandomEmployees(50);

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println("######## DO NOT USE grpoupByKey() for large data ########");
        System.out.println("Unsorted employees groupBy country using very inefficient grpoupByKey() -");

        sc.parallelize(inputData)
                .mapToPair(e -> new Tuple2<>(e.getCountry(), 1L))
                .groupByKey()
                .foreach(t -> System.out.println(t._1 + " -> " + Iterables.size(t._2)));

        sc.close();
    }

}


