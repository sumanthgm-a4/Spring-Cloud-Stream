package com.sum.spring_cloud_stream.sec07.processor;

import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import com.sum.spring_cloud_stream.sec07.dto.Notification;
import com.sum.spring_cloud_stream.sec07.dto.NotificationChannel;
import com.sum.spring_cloud_stream.sec07.dto.Order;
import com.sum.spring_cloud_stream.sec07.dto.Payment;
import com.sum.spring_cloud_stream.sec07.dto.ProductType;
import com.sum.spring_cloud_stream.sec07.dto.Shipment;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class ProcessorConfig {

    @Bean // 1-to-1 mapping
    public Function<Order, Payment> paymentProcessor() {
        return order -> {
            var payment = new Payment(order.id(), order.amount(), UUID.randomUUID());
            log.info("PROCESSING: {}", payment);
            return payment;
        };
    }

    @Bean // 1-to-0/1: filter - spring cloud stream drops the message on null
    public Function<Order, Shipment> shipmentProcessor() {
        return order -> {
            if(ProductType.PHYSICAL.equals(order.productType())){
                return new Shipment(order.id(), "FEDEX-" + order.id());
            }
            return null;
        };
    }

    @Bean // 1-to-N mapping
    public Function<Order, List<Message<Notification>>> notificationProcessor() {
        return order -> List.of(
                MessageBuilder.withPayload(createSMSNotification(order)).build(),
                MessageBuilder.withPayload(createEmailNotification(order)).build()
        );
    }

    private Notification createSMSNotification(Order order) {
        return new Notification(order.id(), NotificationChannel.SMS, String.valueOf(9_000_000_000L + order.customerId()));
    }

    private Notification createEmailNotification(Order order) {
        return new Notification(order.id(), NotificationChannel.EMAIL, "user.%d@gmail.com".formatted(order.customerId()));
    }

}
