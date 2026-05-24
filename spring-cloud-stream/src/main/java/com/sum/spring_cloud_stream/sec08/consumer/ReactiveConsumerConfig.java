package com.sum.spring_cloud_stream.sec08.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.sum.spring_cloud_stream.sec08.dto.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

@Configuration
public class ReactiveConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(ReactiveConsumerConfig.class);

    @Bean
    public Function<Flux<Payment>, Mono<Void>> paymentConsumer() {
        return this::logReceived;
    }

    @Bean
    public Function<Flux<Shipment>, Mono<Void>> shipmentConsumer() {
        return this::logReceived;
    }

    @Bean
    public Function<Flux<Notification>, Mono<Void>> notificationConsumer() {
        return this::logReceived;
    }

    private <T> Mono<Void> logReceived(Flux<T> flux) {
       return flux.doOnNext(item -> log.info("received: {}", item))
                .then();
    }

}