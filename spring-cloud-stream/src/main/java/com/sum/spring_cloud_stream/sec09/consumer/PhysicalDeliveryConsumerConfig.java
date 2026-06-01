package com.sum.spring_cloud_stream.sec09.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.sum.spring_cloud_stream.sec09.dto.PhysicalDelivery;

import java.util.function.Consumer;

@Configuration
public class PhysicalDeliveryConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(PhysicalDeliveryConsumerConfig.class);

    @Bean
    public Consumer<PhysicalDelivery> physicalConsumer() {
        return msg -> log.info("received: {}", msg);
    }

}