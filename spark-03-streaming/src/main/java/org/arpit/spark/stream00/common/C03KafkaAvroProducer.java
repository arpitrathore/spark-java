package org.arpit.spark.stream00.common;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.arpit.spark.common.util.DockerEventUtil;
import org.arpit.spark.schema.avro.DockerEventAvro;

import java.util.Properties;

/**
 * For Avro, use docker image rather than kafka/zookeeper binaries
 * Documentation : https://hub.docker.com/r/landoop/fast-data-dev
 * Docker command : docker run -d -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 -p 9581-9585:9581-9585 -p 9092:9092 -e ADV_HOST=localhost lensesio/fast-data-dev:2.2
 * Access UI : localhost:3030
 */
public class C03KafkaAvroProducer {

    private static int SLEEP_INTERVAL = 1500;

    public static final String KAFKA_BROKERS = "localhost:9092";
    public static final String SCHEMA_REGISTRY = "http://localhost:8081";
    public static final String KAFKA_TOPIC = "docker-event-avro";
    private static int count = 0;

    public static void main(String[] args) throws Exception {
        Properties properties = buildKafkaProperties();

        Producer<String, DockerEventAvro> producer = new KafkaProducer<>(properties);
        while (true) {
            DockerEventAvro dockerEvent = DockerEventUtil.buildRandomDockerEvent();

            ProducerRecord<String, DockerEventAvro> producerRecord = new ProducerRecord<>(
                    KAFKA_TOPIC, (System.currentTimeMillis() + ""), dockerEvent
            );

            producer.send(producerRecord, (metadata, e) -> {
                if (e == null) {
                    System.out.println("DockerEvent avro message sent. Count : " + ++count);
                    //System.out.println("Message : " + dockerEvent);
                } else {
                    e.printStackTrace();
                }
            });
            producer.flush();

            Thread.sleep(SLEEP_INTERVAL);
        }
    }

    private static Properties buildKafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_BROKERS);
        properties.setProperty("min.insync.replicas", "2");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "2");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", SCHEMA_REGISTRY);
        return properties;
    }

}
