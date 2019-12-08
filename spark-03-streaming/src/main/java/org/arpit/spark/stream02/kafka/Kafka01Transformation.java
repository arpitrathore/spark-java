package org.arpit.spark.stream02.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.arpit.spark.common.util.LoggerUtil;
import org.arpit.spark.stream00.common.C02KafkaJsonProducer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Documentation : https://spark.apache.org/docs/2.4.0/streaming-kafka-0-10-integration.html
 * Run org.arpit.spark.stream00.common.C02KafkaJsonProducer.main() to produce employee json on kafka
 */
public class Kafka01Transformation {

    private static final String APP_NAME = Kafka01Transformation.class.getName();

    public static void main(String[] args) throws Exception {
        LoggerUtil.disableSparkLogs();

        final SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");

        final JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        final JavaInputDStream<ConsumerRecord<String, String>> kafkaDStream =
                KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), buildConsumerStrategy());

        final JavaDStream<String> kafkaMessageStream = kafkaDStream.map(e -> e.value());

        kafkaMessageStream.print();

        jssc.start();
        jssc.awaitTermination();
    }


    private static ConsumerStrategy<String, String> buildConsumerStrategy() {
        final Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", C02KafkaJsonProducer.KAFKA_BROKERS);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", APP_NAME);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        final List<String> TOPIC_NAME = Arrays.asList(C02KafkaJsonProducer.KAFKA_TOPIC);
        return ConsumerStrategies.<String, String>Subscribe(TOPIC_NAME, kafkaParams);
    }


}
