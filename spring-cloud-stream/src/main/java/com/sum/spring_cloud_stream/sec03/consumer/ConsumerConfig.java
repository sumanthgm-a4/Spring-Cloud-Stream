package com.sum.spring_cloud_stream.sec03.consumer;

import java.util.function.Consumer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class ConsumerConfig {
    
    @Bean
    public Consumer<String> consumer() {
        return msg -> log.info("RECEIVED: {}", msg);
    }

    @Bean
    public Consumer<Message<String>> messageConsumer() {
        return this::handleMessage;
    }

    private void handleMessage(Message<String> message) {
        log.info("message: {}", message);
        var key = message.getHeaders().get(KafkaHeaders.RECEIVED_KEY);
        var payload = message.getPayload();
        log.info("key: {}, payload: {}", key, payload);
    }

}
