package com.distributedmq.spring.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "mq")
public class MQProperties {
    private String nameserverAddr = "http://localhost:9090";
    private String defaultTopic = "default";
    private int producerAcks = 1;
    private int producerMaxRetries = 3;

    public String getNameserverAddr() {
        return nameserverAddr;
    }

    public void setNameserverAddr(String nameserverAddr) {
        this.nameserverAddr = nameserverAddr;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }

    public int getProducerAcks() {
        return producerAcks;
    }

    public void setProducerAcks(int producerAcks) {
        this.producerAcks = producerAcks;
    }

    public int getProducerMaxRetries() {
        return producerMaxRetries;
    }

    public void setProducerMaxRetries(int producerMaxRetries) {
        this.producerMaxRetries = producerMaxRetries;
    }
}