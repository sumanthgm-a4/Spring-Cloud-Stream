package com.sum.spring_cloud_stream.sec07.producer;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.sum.spring_cloud_stream.sec07.dto.Order;
import com.sum.spring_cloud_stream.sec07.dto.ProductType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class ProducerConfig {

    @Bean
    public Supplier<Order> producer() {
        var counter = new AtomicInteger(0);

        return () -> {
            var id = counter.incrementAndGet();
            var productType = id % 2 == 0 ? ProductType.PHYSICAL : ProductType.DIGITAL;
            var order = new Order(id, id, ThreadLocalRandom.current().nextInt(1, 1000), productType);
            log.info("PRODUCED: {}", order);
            return order;
        };
    }
}
