package com.sum.spring_cloud_stream.sec10.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.sum.spring_cloud_stream.sec10.dto.Order;
import com.sum.spring_cloud_stream.sec10.dto.ProductType;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Configuration
public class ProducerConfig {

    private static final Logger log = LoggerFactory.getLogger(ProducerConfig.class);

    @Bean
    public Supplier<Order> producer() {
        var counter = new AtomicInteger(0);
        return () -> {
            var id = counter.incrementAndGet();
            var productType = id % 2 == 0 ? ProductType.PHYSICAL : ProductType.DIGITAL;
            var order = new Order(id, id, ThreadLocalRandom.current().nextInt(1, 1000), productType);
            log.info("produced: {}", order);
            return order;
        };
    }

}