package org.arpit.spark.structured01.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.arpit.spark.common.util.LoggerUtil;

/**
 * Guide : https://spark.apache.org/docs/2.4.0/structured-streaming-programming-guide.html
 */
public class Structured01Basic {

    private static final String APP_NAME = Structured01Basic.class.getName();

    public static void main(String[] args) {
        LoggerUtil.disableSparkLogs();

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");
        //SparkSession session = new SparkSession(conf);

    }
}
