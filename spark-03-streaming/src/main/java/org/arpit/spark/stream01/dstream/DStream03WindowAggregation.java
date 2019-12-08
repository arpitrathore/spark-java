package org.arpit.spark.stream01.dstream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.arpit.spark.common.util.Employee;
import org.arpit.spark.common.util.LoggerUtil;
import scala.Tuple2;

/**
 * Run org.arpit.spark.stream00.common.C01SocketProducer.main() to produce employee json on port 10000
 */
public class DStream03WindowAggregation {

    private static final String APP_NAME = DStream03WindowAggregation.class.getName();

    public static void main(String[] args) throws Exception {
        LoggerUtil.disableSparkLogs();

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");

        final JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        final JavaReceiverInputDStream<String> employeeStream = jssc.socketTextStream("localhost", 10000);

        final JavaPairDStream<String, Long> cityCountDStream = employeeStream
                .map(e -> Employee.fromJson(e))
                .mapToPair(e -> new Tuple2<>(e.getCity(), 1L))
                .reduceByKeyAndWindow(((v1, v2) -> v1 + v2), Durations.seconds(30));

        cityCountDStream.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
