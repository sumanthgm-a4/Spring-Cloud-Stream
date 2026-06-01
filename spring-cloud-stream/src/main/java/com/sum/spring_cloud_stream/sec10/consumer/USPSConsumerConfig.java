package com.sum.spring_cloud_stream.sec10.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.sum.spring_cloud_stream.sec10.dto.PhysicalDelivery;

import java.util.function.Consumer;

@Configuration
public class USPSConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(USPSConsumerConfig.class);

    @Bean
    public Consumer<PhysicalDelivery> uspsConsumer() {
        return msg -> log.info("received: {}", msg);
    }

}