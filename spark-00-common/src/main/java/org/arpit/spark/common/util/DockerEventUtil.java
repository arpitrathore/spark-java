package org.arpit.spark.common.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.arpit.spark.schema.avro.DockerEventAvro;

import java.util.*;

public class DockerEventUtil {

    private static final List<String> IMAGE_NAMES = Arrays.asList("alpine", "nginx", "httpd", "busybox", "redis",
            "memcached", "mysql", "postgres", "node", "registry", "golang", "hello-world", "centos", "consul", "php",
            "mariadb", "elasticsearch", "docker", "haproxy", "wordpress", "rabbitmq", "ruby", "python", "openjdk",
            "traefik", "logstash", "debian", "tomcat", "influxdb", "java", "swarm", "jenkins", "maven", "kibana",
            "nextcloud", "ghost", "telegraf", "cassandra", "kong", "drupal", "nats", "vault", "owncloud", "fedora",
            "jruby", "sonarqube", "gradle", "sentry", "solr", "perl", "rethinkdb", "neo4j", "amazonlinux", "groovy",
            "percona", "rocket.chat", "chronograf", "buildpack-deps", "ubuntu", "mongo");

    public static DockerEventAvro buildRandomDockerEvent() {
        return buildRandomDockerEvents(1).get(0);
    }

    public static List<DockerEventAvro> buildRandomDockerEvents(int size) {
        final Random random = new Random();
        final List<DockerEventAvro> dockerEvents = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            final DockerEventAvro dockerEvent = DockerEventAvro.newBuilder()
                    .setId(DigestUtils.sha256Hex(UUID.randomUUID().toString() + "#" + System.nanoTime()))
                    .setStatus("start")
                    .setFrom(IMAGE_NAMES.get(random.nextInt(IMAGE_NAMES.size())))
                    .setType("container")
                    .setAction("start")
                    .setScope("local")
                    .setTimeMillis(System.currentTimeMillis())
                    .setTimeNano(System.nanoTime() + "")
                    .build();
            dockerEvents.add(dockerEvent);
        }
        return dockerEvents;
    }
}
