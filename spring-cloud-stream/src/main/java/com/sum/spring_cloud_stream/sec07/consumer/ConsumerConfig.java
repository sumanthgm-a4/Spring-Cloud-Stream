package com.sum.spring_cloud_stream.sec07.consumer;

import java.util.function.Consumer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.sum.spring_cloud_stream.sec07.dto.Notification;
import com.sum.spring_cloud_stream.sec07.dto.Payment;
import com.sum.spring_cloud_stream.sec07.dto.Shipment;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class ConsumerConfig {

    @Bean
    public Consumer<Payment> paymentConsumer() {
        return this::logReceived;
    }

    @Bean
    public Consumer<Shipment> shipmentConsumer() {
        return this::logReceived;
    }

    @Bean
    public Consumer<Notification> notificationConsumer() {
        return this::logReceived;
    }

    private void logReceived(Object payload) {
        log.info("RECEIVED: {}", payload);
    }
}
