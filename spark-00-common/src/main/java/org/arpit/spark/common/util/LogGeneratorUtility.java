package org.arpit.spark.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import org.arpit.spark.common.pojo.LogMessage;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class LogGeneratorUtility {

    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss");
    private static final List<Integer> responseCodes = Arrays.asList(200, 401, 404, 500, 301);
    private static final List<String> verbs = Arrays.asList("GET", "POST", "DELETE", "PUT");
    private static final List<String> resources = Arrays.asList("/list", "/wp-content", "/wp-admin", "/explore", "/search/tag/list", "/app/main/posts", "/posts/posts/explore", "/apps/cart.jsp?appID=");


    public static String generateRandomLogJson() {
        try {
            return OBJECT_MAPPER.writeValueAsString(generateRandomLogMessages(1).get(0));
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }

    public static String generateRandomLogRaw() {
        return generateRandomLogMessages(1).get(0).buildRawLog();
    }

    private static List<LogMessage> generateRandomLogMessages(int count) {
        final List<LogMessage> logs = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final Faker faker = new Faker();
            final Random random = new Random();
            final String ip = faker.internet().ipV4Address();
            final String date = DATE_FORMAT.format(new Date()) + " -0530";
            final String verb = verbs.get(random.nextInt(verbs.size()));
            String resource = resources.get(random.nextInt(resources.size()));
            resource = resource.contains("apps") ? resource + random.nextInt(50000) : resource;
            final Integer responseCode = responseCodes.get(random.nextInt(responseCodes.size()));
            final Long responseBytes = Long.valueOf(random.nextInt(1850) + 150);
            final String referer = faker.internet().url();
            final String userAgent = faker.internet().userAgentAny();

            //213.43.166.91 - - [31/Dec/2019:10:11:54 +0000] "POST /posts/posts/explore HTTP/1.1" 200 "www.dennis-hodkiewicz.biz" "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.0; Windows NT 5.1; .NET CLR 1.1.4322)" 219
            logs.add(new LogMessage(ip, date, verb, resource, responseCode, referer, userAgent, responseBytes));
        }
        return logs;
    }
}
