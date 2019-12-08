package org.arpit.spark.stream00.common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.arpit.spark.common.util.Employee;

import java.util.Properties;

/**
 * Run this command to create topic in kafka
 * bin/kafka-topics.sh --zookeeper localhost:2181 --create --replication-factor 1 --partitions 5 --topic employee-spark-topic
 */
public class C02KafkaJsonProducer {

    private static final int KAFKA_MESSAGE_COUNT = 5;

    public static final String KAFKA_BROKERS = "localhost:9092";
    public static final String KAFKA_TOPIC = "employee-spark-topic";

    public static void main(String[] args) throws Exception {
        Properties properties = buildKafkaProperties();

        Producer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < KAFKA_MESSAGE_COUNT; i++) {
            String employeeJson = Employee.buildRandomEmployeeJson();
            System.out.println(employeeJson);

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                    KAFKA_TOPIC, (System.currentTimeMillis() + ""), employeeJson
            );

            //System.out.println(employee);
            producer.send(producerRecord, (metadata, e) -> {
                if (e != null) {
                    e.printStackTrace();
                }
            });
            producer.flush();
        }
        producer.close();
    }

    private static Properties buildKafkaProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}
