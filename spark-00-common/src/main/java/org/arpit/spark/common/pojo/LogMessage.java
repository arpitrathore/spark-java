package org.arpit.spark.common.pojo;

import java.io.Serializable;

public class LogMessage implements Serializable {
    private String ip;
    private String date;
    private String verb;
    private String resource;
    private Integer responseCode;
    private String referer;
    private String userAgent;
    private Long responseBytes;

    public LogMessage() {

    }

    public LogMessage(String ip, String date, String verb, String resource, Integer responseCode,
                      String referer, String userAgent, Long responseBytes) {
        this.ip = ip;
        this.date = date;
        this.verb = verb;
        this.resource = resource;
        this.responseCode = responseCode;
        this.responseBytes = responseBytes;
        this.referer = referer;
        this.userAgent = userAgent;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getVerb() {
        return verb;
    }

    public void setVerb(String verb) {
        this.verb = verb;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public Integer getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(Integer responseCode) {
        this.responseCode = responseCode;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public Long getResponseBytes() {
        return responseBytes;
    }

    public void setResponseBytes(Long responseBytes) {
        this.responseBytes = responseBytes;
    }

    public String buildRawLog() {
        return String.format("%s - - [%s] \"%s %s HTTP/1.1\" %d \"%s\" \"%s\" %d",
                ip, date, verb, resource, responseCode, referer, userAgent, responseBytes);
    }
}
