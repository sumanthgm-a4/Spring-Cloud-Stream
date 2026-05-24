package com.sum.spring_cloud_stream.sec05.producer;

import java.time.Duration;
import java.util.function.Supplier;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
@Configuration
public class ReactiveProducerConfig {

    @Bean
    public Supplier<Flux<String>> reactiveProducer() {
        return () -> Flux.interval(Duration.ofMillis(500))
            .map(i -> "msg-" + i)
            .doOnNext(msg -> log.info("SENDING: {}", msg));
    }

}
