package com.distributedmq.spring.autoconfigure;

import com.distributedmq.client.MQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MQTemplate {
    private static final Logger logger = LoggerFactory.getLogger(MQTemplate.class);

    private final MQProducer producer;
    private final MQProperties properties;

    @Autowired
    public MQTemplate(MQProducer producer, MQProperties properties) {
        this.producer = producer;
        this.properties = properties;
    }

    public void send(String topic, int partition, String message) throws Exception {
        producer.send(topic, partition, message.getBytes(), properties.getProducerAcks(), properties.getProducerMaxRetries());
        logger.debug("Sent message to topic={}, partition={}", topic, partition);
    }

    public void send(String topic, int partition, byte[] message) throws Exception {
        producer.send(topic, partition, message, properties.getProducerAcks(), properties.getProducerMaxRetries());
        logger.debug("Sent message to topic={}, partition={}", topic, partition);
    }

    public void send(String topic, String message) throws Exception {
        send(topic, 0, message);
    }

    public void send(String message) throws Exception {
        send(properties.getDefaultTopic(), 0, message);
    }
}