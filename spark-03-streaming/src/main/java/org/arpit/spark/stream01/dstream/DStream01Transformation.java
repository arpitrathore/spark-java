package org.arpit.spark.stream01.dstream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class DStream01Transformation {

    private static final String APP_NAME = DStream01Transformation.class.getName();

    //Run org.arpit.spark.stream01.common.SocketProducer.main() to produce employee json on port 10000
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");

        final JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        final JavaReceiverInputDStream<String> employeeStream = jssc.socketTextStream("localhost", 10000);

        final JavaDStream<String> dStream = employeeStream.map(e -> e);

        dStream.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
