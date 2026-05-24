package com.sum.spring_cloud_stream.sec06.consumer;

import java.util.function.Consumer;

import org.apache.kafka.common.metrics.Measurable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class ConsumerConfig {

    @Bean
    public Consumer<String> consumer(MessageCounter counter) {
        return msg -> {
            counter.increment();
            log.info("RECEIVED: {}", msg);
        };
    }
}
