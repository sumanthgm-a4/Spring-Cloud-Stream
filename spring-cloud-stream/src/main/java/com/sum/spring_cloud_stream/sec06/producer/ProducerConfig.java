package com.sum.spring_cloud_stream.sec06.producer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class ProducerConfig {

    @Bean
    public Supplier<String> producer() {
        var counter = new AtomicInteger(0);

        return () -> {
            var msg = "msg-" + counter.incrementAndGet();
            log.info("PRODUCED: {}", msg);
            return msg;
        };
    }
}
