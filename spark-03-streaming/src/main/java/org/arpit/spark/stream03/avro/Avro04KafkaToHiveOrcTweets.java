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
import org.apache.spark.sql.SparkSession;
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
import org.arpit.spark.stream00.common.C04KafkaAvroProducerTweets;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Documentation : https://spark.apache.org/docs/2.4.0/streaming-kafka-0-10-integration.html
 * Run {@link org.arpit.spark.stream00.common.C04KafkaAvroProducerTweets} to produce random pull tweets and push to kafka in avro format
 * Create hive table : "create table tweets(id string, timeMillis bigint, tweetText string, language string, favorited boolean,
 * favoriteCount bigint, retweeted boolean, retweetCount bigint, replyCount bigint, possiblySensitive boolean, userId string,
 * userScreenName string, userLocation string, userVerified boolean, userFollowerCount bigint) STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY");"
 */
public class Avro04KafkaToHiveOrcTweets {
    private static final String APP_NAME = Avro04KafkaToHiveOrcTweets.class.getName();

    public static void main(String[] args) throws Exception {
        LoggerUtil.disableSparkLogs();

        final SparkConf conf = new SparkConf()
                .setAppName(APP_NAME)
                .setMaster("local[*]")
                .set("hive.metastore.uris", "thrift://localhost:9083");

        SparkSession session = SparkSession.builder()
                .appName(APP_NAME)
                .master("local[*]")
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(session.sparkContext());
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkContext, Durations.seconds(5));
        final SQLContext sqlContext = session.sqlContext();

        final JavaInputDStream<ConsumerRecord<String, GenericData.Record>> kafkaDStream =
                KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), buildConsumerStrategy());


        final StructType structType = C00AvroUtil
                .buildDataFrameSchemaFromSchemaRegistrySchema(C04KafkaAvroProducerTweets.SCHEMA_REGISTRY,
                        C04KafkaAvroProducerTweets.TWITTER_KAFKA_TOPIC, 1);
        System.out.println("Struct type built from schema registry schema" + structType);

        kafkaDStream.foreachRDD(message -> {
            final JavaRDD<String> rows = message.map(record -> record.value().toString());
            if (rows.count() > 0) {
                Dataset<Row> df = sqlContext.read().schema(structType).json(rows);
                df.show();
                df.coalesce(1)
                        .write()
                        .mode("append")
                        .format("hive")
                        .option("compression", "snappy")
                        .insertInto("tweets");
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }

    private static ConsumerStrategy<String, GenericData.Record> buildConsumerStrategy() {
        final Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", C04KafkaAvroProducerTweets.KAFKA_BROKERS);
        kafkaParams.put("schema.registry.url", C04KafkaAvroProducerTweets.SCHEMA_REGISTRY);

        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", KafkaAvroDeserializer.class);

        kafkaParams.put("group.id", APP_NAME);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        final List<String> TOPIC_NAME = Arrays.asList(C04KafkaAvroProducerTweets.TWITTER_KAFKA_TOPIC);
        return ConsumerStrategies.<String, GenericData.Record>Subscribe(TOPIC_NAME, kafkaParams);
    }


}
