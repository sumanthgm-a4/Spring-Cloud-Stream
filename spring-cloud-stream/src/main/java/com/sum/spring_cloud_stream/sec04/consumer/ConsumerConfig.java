package com.sum.spring_cloud_stream.sec04.consumer;

import java.util.function.Consumer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class ConsumerConfig {
    @Bean
    public Consumer<String> consumer() {
        return msg -> log.info("RECEIVED: {}", msg);
    }
}
