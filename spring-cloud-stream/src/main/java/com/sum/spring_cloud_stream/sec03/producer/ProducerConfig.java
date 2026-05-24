package com.sum.spring_cloud_stream.sec03.producer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

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

    @Bean
    public Supplier<Message<String>> messageProducer() {
        var counter = new AtomicInteger(0);

        return () -> {
            var msg = this.buildMessage(counter.incrementAndGet());
            log.info("PRODUCED: {}", msg);
            return msg;
        };
    }

    private Message<String> buildMessage(Integer input) {
        return MessageBuilder.withPayload("msg-" + input)
            .setHeader(KafkaHeaders.KEY, "key-" + input)
            .setHeader("sample-header", "sample-header-" + input)
            .build();
    } 
}
