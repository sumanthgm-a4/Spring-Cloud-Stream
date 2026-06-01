package com.sum.spring_cloud_stream.sec14.processor;

import org.springframework.stereotype.Service;

import com.sum.spring_cloud_stream.sec14.dto.*;

import java.time.Duration;

@Service
public class DeliveryService {

    public Object toDelivery(Order order) {
        this.simulateNetworkCall();
        return switch (order.productType()) {
            case DIGITAL -> this.toDigitalDelivery(order);
            case PHYSICAL -> this.toPhysicalDelivery(order);
        };
    }

    private DigitalDelivery toDigitalDelivery(Order order) {
        return new DigitalDelivery(
                order.id(),
                "user.%d@gmail.com".formatted(order.customerId())
        );
    }

    private PhysicalDelivery toPhysicalDelivery(Order order) {
        return new PhysicalDelivery(
                order.id(),
                "%d street".formatted(order.customerId()),
                "%d city".formatted(order.customerId())
        );
    }

    private void simulateNetworkCall(){
        try {
            Thread.sleep(Duration.ofMillis(200));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}