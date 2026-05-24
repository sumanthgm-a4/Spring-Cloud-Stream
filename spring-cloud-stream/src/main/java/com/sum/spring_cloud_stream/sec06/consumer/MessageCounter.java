package com.sum.spring_cloud_stream.sec06.consumer;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class MessageCounter {

    private final AtomicInteger counter = new AtomicInteger(0);

    public void increment() {
        counter.incrementAndGet();
    }

    @PreDestroy
    public void onShutdown() {
        log.info("Total messages consumed: {}", counter.get());
    }
}
