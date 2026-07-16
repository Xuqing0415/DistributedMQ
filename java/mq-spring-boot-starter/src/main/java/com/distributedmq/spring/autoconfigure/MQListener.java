package com.distributedmq.spring.autoconfigure;

import org.springframework.stereotype.Component;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Component
public @interface MQListener {
    String topic();
    String group();
    String clientId() default "";
}