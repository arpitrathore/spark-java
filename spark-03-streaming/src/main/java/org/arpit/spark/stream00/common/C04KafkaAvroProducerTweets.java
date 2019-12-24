package org.arpit.spark.stream00.common;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.arpit.spark.common.util.TweetUtil;
import org.arpit.spark.schema.avro.DockerEventAvro;
import org.arpit.spark.schema.avro.TweetAvro;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * For Avro, use docker image rather than kafka/zookeeper binaries
 * Documentation : https://hub.docker.com/r/landoop/fast-data-dev
 * Docker command : docker run --restart always -d -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 -p 9581-9585:9581-9585 -p 9092:9092 -e ADV_HOST=localhost lensesio/fast-data-dev:2.2
 * Access UI : localhost:3030
 * <p>
 * Set these environment variables (Either in intellij configuration or your execution environment)
 * TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET
 */
public class C04KafkaAvroProducerTweets {

    private static int SLEEP_INTERVAL = 1500;

    private static final List<String> SEARCH_TERMS = Arrays.asList("modi");

    private static BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

    public static final String KAFKA_BROKERS = "localhost:9092";
    public static final String SCHEMA_REGISTRY = "http://localhost:8081";
    public static final String TWITTER_KAFKA_TOPIC = "tweets";
    private static int count = 0;

    private static String API_KEY = System.getenv("TWITTER_API_KEY");
    private static String API_SECRET = System.getenv("TWITTER_API_SECRET");
    private static String ACCESS_TOKEN = System.getenv("TWITTER_ACCESS_TOKEN");
    private static String ACCESS_TOKEN_SECRET = System.getenv("TWITTER_ACCESS_TOKEN_SECRET");

    public static void main(String[] args) throws Exception {
        validateTwitterTokens();

        Properties properties = buildKafkaProperties();
        Producer<String, TweetAvro> producer = new KafkaProducer<>(properties);

        Client twitterClient = buildTwitterClient();
        twitterClient.connect();

        while (!twitterClient.isDone()) {
            String message = msgQueue.poll(SLEEP_INTERVAL, TimeUnit.MILLISECONDS);
            if (message != null) {
                TweetAvro tweetAvro = TweetUtil.buildTweetAvroFromJson(message);

                ProducerRecord<String, TweetAvro> producerRecord = new ProducerRecord<>(
                        TWITTER_KAFKA_TOPIC, (System.currentTimeMillis() + ""), tweetAvro
                );

                producer.send(producerRecord, (metadata, e) -> {
                    if (e == null) {
                        System.out.println("Tweet avro message sent for search terms " + SEARCH_TERMS + ". Count : " + ++count);
                        //System.out.println("Message : " + dockerEvent);
                    } else {
                        e.printStackTrace();
                    }
                });
                producer.flush();
            }
        }
    }

    private static void validateTwitterTokens() {
        if (API_KEY == null || API_SECRET == null || ACCESS_TOKEN == null || ACCESS_TOKEN_SECRET == null) {
            System.out.println("Set these environment variables : " +
                    "TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET");
            System.exit(1);
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

    private static Client buildTwitterClient() {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(SEARCH_TERMS);
        Authentication twitterAuth = new OAuth1(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);

        ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01").hosts(hosebirdHosts)
                .authentication(twitterAuth).endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

}
