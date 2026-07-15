package com.distributedmq.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "mq")
public class MQProperties {
    private String nameserverAddr = "localhost:9090";

    public String getNameserverAddr() {
        return nameserverAddr;
    }

    public void setNameserverAddr(String nameserverAddr) {
        this.nameserverAddr = nameserverAddr;
    }
}
