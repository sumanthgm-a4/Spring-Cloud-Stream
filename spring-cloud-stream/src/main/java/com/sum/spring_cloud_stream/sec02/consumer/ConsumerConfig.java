package com.sum.spring_cloud_stream.sec02.consumer;

import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@Configuration
public class ConsumerConfig {
    
    @Bean
    public Function<Flux<String>, Mono<Void>> reactiveConsumer() {
        return flux -> flux.doOnNext(msg -> log.info("RECEIVED: {}", msg))
            .then();
    }

}
