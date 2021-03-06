package org.arpit.spark.structured00.common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.arpit.spark.common.util.EmployeeUtil;
import org.arpit.spark.common.util.LogGeneratorUtility;

import java.util.Properties;

/**
 * Run this command to create topic in kafka
 * bin/kafka-topics.sh --zookeeper localhost:2181 --create --replication-factor 1 --partitions 5 --topic log-raw-topic
 */
public class C01KafkaRawLogsProducer {

    private static int SLEEP_INTERVAL = 500;

    public static final String KAFKA_BROKERS = "localhost:9092";
    public static final String KAFKA_TOPIC = "log-raw-topic";

    public static void main(String[] args) throws Exception {
        Properties properties = buildKafkaProperties();

        Producer<String, String> producer = new KafkaProducer<>(properties);
        while (true) {
            String logMessage = LogGeneratorUtility.generateRandomLogRaw();
            System.out.println(logMessage);

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                    KAFKA_TOPIC, (System.currentTimeMillis() + ""), logMessage
            );

            producer.send(producerRecord, (metadata, e) -> {
                if (e != null) {
                    e.printStackTrace();
                }
            });
            producer.flush();

            Thread.sleep(SLEEP_INTERVAL);
        }
    }

    private static Properties buildKafkaProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}
