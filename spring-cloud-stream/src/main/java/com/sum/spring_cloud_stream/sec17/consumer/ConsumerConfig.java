package com.sum.spring_cloud_stream.sec17.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
public class ConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(ConsumerConfig.class);

    @Bean
    public Consumer<String> consumer() {
        return msg -> log.info("received: {}", msg);
    }

}