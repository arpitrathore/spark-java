package org.arpit.spark.stream01.dstream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.arpit.spark.common.util.Employee;
import org.arpit.spark.common.util.LoggerUtil;
import org.arpit.spark.stream00.common.C01SocketProducer;
import scala.Tuple2;

/**
 * Run {@link C01SocketProducer} to produce employee json on port 10000
 */
public class DStream02Aggregation {

    private static final String APP_NAME = DStream02Aggregation.class.getName();

    public static void main(String[] args) throws Exception {
        LoggerUtil.disableSparkLogs();

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");

        final JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        final JavaReceiverInputDStream<String> employeeStream = jssc.socketTextStream("localhost", 10000);

        final JavaPairDStream<String, Long> cityCountDStream = employeeStream
                .map(e -> Employee.fromJson(e))
                .mapToPair(e -> new Tuple2<>(e.getCity(), 1L))
                .reduceByKey((v1, v2) -> v1 + v2);

        cityCountDStream.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
