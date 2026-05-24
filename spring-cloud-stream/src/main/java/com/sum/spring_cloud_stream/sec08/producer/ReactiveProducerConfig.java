package com.sum.spring_cloud_stream.sec08.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;

import com.sum.spring_cloud_stream.sec08.dto.*;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

@Configuration
public class ReactiveProducerConfig {

    private static final Logger log = LoggerFactory.getLogger(ReactiveProducerConfig.class);

    @Bean
    public Supplier<Flux<Order>> producer() {
        return () -> Flux.interval(Duration.ofSeconds(1))
                         .skip(1)
                         .map(id -> this.buildOrder(id.intValue()))
                         .doOnNext(order -> log.info("produced: {}", order));

    }

    private Order buildOrder(int id) {
        var productType = id % 2 == 0 ? ProductType.PHYSICAL : ProductType.DIGITAL;
        return new Order(id, id, ThreadLocalRandom.current().nextInt(1, 1000), productType);
    }

}