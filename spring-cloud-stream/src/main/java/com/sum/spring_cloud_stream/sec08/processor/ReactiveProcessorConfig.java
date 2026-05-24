package com.sum.spring_cloud_stream.sec08.processor;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.sum.spring_cloud_stream.sec08.dto.*;

import reactor.core.publisher.Flux;

import java.util.UUID;
import java.util.function.Function;

@Configuration
public class ReactiveProcessorConfig {

    @Bean // 1-to-1 mapping
    public Function<Flux<Order>, Flux<Payment>> paymentProcessor() {
        return flux -> flux.map(order -> new Payment(order.id(), order.amount(), UUID.randomUUID()));
    }

    @Bean // 1-to-0/1: filter
    public Function<Flux<Order>, Flux<Shipment>> shipmentProcessor() {
        return flux -> flux.filter(order -> ProductType.PHYSICAL.equals(order.productType()))
                           .map(order -> new Shipment(order.id(), "FEDEX-" + order.id()));
    }

    @Bean // 1-to-N mapping
    public Function<Flux<Order>, Flux<Notification>> notificationProcessor() {
        return flux -> flux.flatMap(order -> Flux.just(
                createSMSNotification(order),
                createEmailNotification(order)
        ));
    }

    private Notification createSMSNotification(Order order) {
        return new Notification(order.id(), NotificationChannel.SMS, String.valueOf(9_000_000_000L + order.customerId()));
    }

    private Notification createEmailNotification(Order order) {
        return new Notification(order.id(), NotificationChannel.EMAIL, "user.%d@gmail.com".formatted(order.customerId()));
    }

}