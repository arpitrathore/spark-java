package org.arpit.spark.stream01.dstream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.arpit.spark.common.util.LoggerUtil;

/**
 * Run org.arpit.spark.stream00.common.C01SocketProducer.main() to produce employee json on port 10000
 */
public class DStream01Transformation {

    private static final String APP_NAME = DStream01Transformation.class.getName();

    public static void main(String[] args) throws Exception {
        LoggerUtil.disableSparkLogs();

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");

        final JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        final JavaReceiverInputDStream<String> employeeStream = jssc.socketTextStream("localhost", 10000);

        final JavaDStream<String> dStream = employeeStream.map(e -> e);

        dStream.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
