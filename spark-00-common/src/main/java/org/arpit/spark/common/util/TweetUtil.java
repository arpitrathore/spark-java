package org.arpit.spark.common.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.arpit.spark.schema.avro.TweetAvro;

import java.io.IOException;

public class TweetUtil {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static TweetAvro buildTweetAvroFromJson(String tweetJson) throws IOException {
        JsonNode jsonNode = OBJECT_MAPPER.readTree(tweetJson);

        return TweetAvro.newBuilder()
                .setId(getString(jsonNode, "id_str"))
                .setTimeMillis(getLong(jsonNode, "timestamp_ms"))
                .setTweetText(getString(jsonNode, "text"))
                .setLanguage(getString(jsonNode, "lang"))
                .setFavorited(getBoolean(jsonNode, "favorited"))
                .setFavoriteCount(getLong(jsonNode, "favorite_count"))
                .setRetweeted(getBoolean(jsonNode, "retweeted"))
                .setRetweetCount(getLong(jsonNode, "retweet_count"))
                .setReplyCount(getLong(jsonNode, "reply_count"))
                .setPossiblySensitive(getBoolean(jsonNode, "possibly_sensitive"))
                .setUserId(getString(jsonNode.get("user"), "id_str"))
                .setUserScreenName(getString(jsonNode.get("user"), "screen_name"))
                .setUserLocation(getString(jsonNode.get("user"), "location"))
                .setUserVerified(getBoolean(jsonNode.get("user"), "verified"))
                .setUserFollowerCount(getLong(jsonNode.get("user"), "followers_count"))
                .build();
    }

    private static String getString(JsonNode jsonNode, String fieldName) {
        if (jsonNode.get(fieldName) != null) {
            return jsonNode.get(fieldName).asText();
        }
        return "";
    }

    private static Long getLong(JsonNode jsonNode, String fieldName) {
        if (jsonNode.get(fieldName) != null) {
            return jsonNode.get(fieldName).asLong();
        }
        return 0L;
    }

    private static Boolean getBoolean(JsonNode jsonNode, String fieldName) {
        if (jsonNode.get(fieldName) != null) {
            return jsonNode.get(fieldName).asBoolean();
        }
        return false;
    }

    /*
    Sample tween json
    {
             "created_at": "Tue Dec 24 09:25:17 +0000 2019",
            "id": 1209404669962965000,
            "id_str": "1209404669962964992",
            "text": "Amit Shah is bringing Modi down and the BJP party too! @bainjal https://t.co/WVXxuxTxRH via @ndtv",
            "source": "<a href=\"http://twitter.com\" rel=\"nofollow\">Twitter Web Client</a>",
            "truncated": false,
            "in_reply_to_status_id": null,
            "in_reply_to_status_id_str": null,
            "in_reply_to_user_id": null,
            "in_reply_to_user_id_str": null,
            "in_reply_to_screen_name": null,
            "user": {
        "id": 70355674,
                "id_str": "70355674",
                "name": "Ashok Swain",
                "screen_name": "ashoswai",
                "location": "Uppsala, Sweden",
                "url": "http://katalog.uu.se/profile/?id=AA64",
                "description": "Professor of Peace and Conflict Research, Uppsala University, Sweden. @UU_Peace Views are My Own.",
                "translator_type": "none",
                "protected": false,
                "verified": true,
                "followers_count": 148514,
                "friends_count": 1591,
                "listed_count": 412,
                "favourites_count": 24594,
                "statuses_count": 42264,
                "created_at": "Mon Aug 31 09:42:29 +0000 2009",
                "utc_offset": null,
                "time_zone": null,
                "geo_enabled": true,
                "lang": null,
                "contributors_enabled": false,
                "is_translator": false,
                "profile_background_color": "000000",
                "profile_background_image_url": "http://abs.twimg.com/images/themes/theme1/bg.png",
                "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme1/bg.png",
                "profile_background_tile": false,
                "profile_link_color": "91D2FA",
                "profile_sidebar_border_color": "000000",
                "profile_sidebar_fill_color": "000000",
                "profile_text_color": "000000",
                "profile_use_background_image": false,
                "profile_image_url": "http://pbs.twimg.com/profile_images/1111360132783489024/gXWDvVZW_normal.png",
                "profile_image_url_https": "https://pbs.twimg.com/profile_images/1111360132783489024/gXWDvVZW_normal.png",
                "profile_banner_url": "https://pbs.twimg.com/profile_banners/70355674/1525348824",
                "default_profile": false,
                "default_profile_image": false,
                "following": null,
                "follow_request_sent": null,
                "notifications": null
    },
        "geo": null,
            "coordinates": null,
            "place": null,
            "contributors": null,
            "is_quote_status": false,
            "quote_count": 0,
            "reply_count": 0,
            "retweet_count": 0,
            "favorite_count": 0,
            "entities": {
        "hashtags": [],
        "urls": [
        {
            "url": "https://t.co/WVXxuxTxRH",
                "expanded_url": "https://www.ndtv.com/opinion/amit-shah-10-is-defining-modi-20-with-big-consequences-2153533",
                "display_url": "ndtv.com/opinion/amit-s…",
                "indices": [
            64,
                    87
        ]
        }
    ],
        "user_mentions": [
        {
            "screen_name": "bainjal",
                "name": "Swati Chaturvedi",
                "id": 89732309,
                "id_str": "89732309",
                "indices": [
            55,
                    63
        ]
        },
        {
            "screen_name": "ndtv",
                "name": "NDTV",
                "id": 37034483,
                "id_str": "37034483",
                "indices": [
            92,
                    97
        ]
        }
    ],
        "symbols": []
    },
        "favorited": false,
            "retweeted": false,
            "possibly_sensitive": false,
            "filter_level": "low",
            "lang": "en",
            "timestamp_ms": "1577179517628"
    }*/
}
