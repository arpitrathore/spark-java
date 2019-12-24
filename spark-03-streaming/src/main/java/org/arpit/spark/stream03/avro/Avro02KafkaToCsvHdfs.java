package org.arpit.spark.stream03.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.arpit.spark.common.util.LoggerUtil;
import org.arpit.spark.stream00.common.C00AvroUtil;
import org.arpit.spark.stream00.common.C03KafkaAvroProducerDockerEvents;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Documentation : https://spark.apache.org/docs/2.4.0/streaming-kafka-0-10-integration.html
 * Run {@link C03KafkaAvroProducerDockerEvents} to produce random docker events in avro format
 */
public class Avro02KafkaToCsvHdfs {
    private static final String APP_NAME = Avro02KafkaToCsvHdfs.class.getName();

    public static void main(String[] args) throws Exception {
        LoggerUtil.disableSparkLogs();

        final SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");
        final JavaSparkContext sparkContext = new JavaSparkContext(conf);
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkContext, Durations.seconds(5));
        final SQLContext sqlContext = new SQLContext(sparkContext);

        final JavaInputDStream<ConsumerRecord<String, GenericData.Record>> kafkaDStream =
                KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), buildConsumerStrategy());


        final StructType structType = C00AvroUtil
                .buildDataFrameSchemaFromSchemaRegistrySchema(C03KafkaAvroProducerDockerEvents.SCHEMA_REGISTRY,
                        C03KafkaAvroProducerDockerEvents.KAFKA_TOPIC, 1);
        System.out.println("Struct type built from schema registry schema" + structType);

        kafkaDStream.foreachRDD(message -> {
            final JavaRDD<String> rows = message.map(record -> record.value().toString());
            if (rows.count() > 0) {
                Dataset<Row> df = sqlContext.read().schema(structType).json(rows);
                df.show();
                df.write().format("csv").mode(SaveMode.Append).save("hdfs://localhost:9000/docker-events-dir");
                System.out.println("Data written to csv at : hdfs://localhost:9000/docker-events-dir");
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }

    private static ConsumerStrategy<String, GenericData.Record> buildConsumerStrategy() {
        final Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", C03KafkaAvroProducerDockerEvents.KAFKA_BROKERS);
        kafkaParams.put("schema.registry.url", C03KafkaAvroProducerDockerEvents.SCHEMA_REGISTRY);

        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", KafkaAvroDeserializer.class);

        kafkaParams.put("group.id", APP_NAME);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        final List<String> TOPIC_NAME = Arrays.asList(C03KafkaAvroProducerDockerEvents.KAFKA_TOPIC);
        return ConsumerStrategies.<String, GenericData.Record>Subscribe(TOPIC_NAME, kafkaParams);
    }


}
