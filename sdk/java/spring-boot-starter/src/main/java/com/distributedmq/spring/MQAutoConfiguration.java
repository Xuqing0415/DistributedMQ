package com.distributedmq.spring;

import com.distributedmq.client.MQClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(MQProperties.class)
public class MQAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public MQClient mqClient(MQProperties properties) {
        return new MQClient(properties.getNameserverAddr());
    }
}
