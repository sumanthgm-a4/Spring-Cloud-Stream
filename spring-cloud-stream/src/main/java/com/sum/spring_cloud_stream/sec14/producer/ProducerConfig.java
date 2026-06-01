package com.sum.spring_cloud_stream.sec14.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.sum.spring_cloud_stream.sec09.dto.Order;
import com.sum.spring_cloud_stream.sec09.dto.ProductType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Configuration
public class ProducerConfig {

    private static final Logger log = LoggerFactory.getLogger(ProducerConfig.class);

    @Bean
    public Supplier<Message<Order>> producer() {
        var counter = new AtomicInteger(0);
        return () -> {
            var message = this.toMessage(counter.incrementAndGet());
            log.info("produced: {}", message);
            return message;
        };
    }

    private Message<Order> toMessage(int input) {
        var productType = input % 2 == 0 ? ProductType.PHYSICAL : ProductType.DIGITAL;
        var order = new Order(input, input % 50, ThreadLocalRandom.current().nextInt(1, 1000), productType);
        return MessageBuilder.withPayload(order).setHeader(KafkaHeaders.KEY, input).build();
    }

}