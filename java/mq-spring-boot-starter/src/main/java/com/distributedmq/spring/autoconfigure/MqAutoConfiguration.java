package com.distributedmq.spring.autoconfigure;

import com.distributedmq.client.MQConsumer;
import com.distributedmq.client.MQProducer;
import com.distributedmq.client.model.MQMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
@EnableConfigurationProperties(MQProperties.class)
public class MqAutoConfiguration implements ApplicationContextAware {
    private static final Logger logger = LoggerFactory.getLogger(MqAutoConfiguration.class);

    private ApplicationContext applicationContext;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final List<MQConsumer> consumers = new ArrayList<>();

    @Autowired
    private MQProperties mqProperties;

    @Bean(destroyMethod = "close")
    public MQProducer mqProducer() {
        logger.info("Initializing MQProducer with nameserver: {}", mqProperties.getNameserverAddr());
        return new MQProducer(mqProperties.getNameserverAddr());
    }

    @Bean(destroyMethod = "shutdown")
    public MQTemplate mqTemplate(MQProducer producer) {
        return new MQTemplate(producer, mqProperties);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        registerListeners();
    }

    private void registerListeners() {
        Map<String, Object> beansWithAnnotation = applicationContext.getBeansWithAnnotation(MQListener.class);

        for (Map.Entry<String, Object> entry : beansWithAnnotation.entrySet()) {
            Object bean = entry.getValue();
            Class<?> beanClass = bean.getClass();

            for (Method method : beanClass.getDeclaredMethods()) {
                MQListener annotation = method.getAnnotation(MQListener.class);
                if (annotation != null) {
                    registerListener(bean, method, annotation);
                }
            }
        }
    }

    private void registerListener(final Object bean, final Method method, MQListener annotation) {
        final String topic = annotation.topic();
        String group = annotation.group();
        String clientId = annotation.clientId();
        if (clientId.isEmpty()) {
            clientId = UUID.randomUUID().toString().substring(0, 8);
        }
        final String finalGroup = group;
        final String finalClientId = clientId;

        logger.info("Registering MQListener: topic={}, group={}, clientId={}", topic, group, clientId);

        executorService.submit(() -> {
            final MQConsumer consumer = new MQConsumer(mqProperties.getNameserverAddr(), finalGroup, finalClientId, topic);
            consumers.add(consumer);

            try {
                consumer.joinGroup();

                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        for (MQMessage message : consumer.poll(1000)) {
                            try {
                                method.invoke(bean, message);
                                consumer.commitOffset(message.getPartition(), message.getOffset() + 1);
                            } catch (Exception e) {
                                logger.error("Failed to process message: {}", e.getMessage());
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("Poll failed: {}", e.getMessage());
                        Thread.sleep(1000);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Listener thread interrupted for topic={}", topic);
            } catch (Exception e) {
                logger.error("Listener thread failed: {}", e.getMessage());
            } finally {
                try {
                    consumer.leaveGroup();
                } catch (Exception e) {
                    logger.warn("Failed to leave group: {}", e.getMessage());
                }
                consumers.remove(consumer);
            }
        });
    }

    @PreDestroy
    public void destroy() {
        logger.info("Shutting down MQ listeners...");
        executorService.shutdownNow();

        for (MQConsumer consumer : consumers) {
            try {
                consumer.leaveGroup();
            } catch (Exception e) {
                logger.warn("Failed to shutdown consumer: {}", e.getMessage());
            }
        }

        consumers.clear();
        logger.info("MQ listeners shutdown complete");
    }
}