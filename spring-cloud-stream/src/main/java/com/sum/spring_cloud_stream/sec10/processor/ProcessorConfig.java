package com.sum.spring_cloud_stream.sec10.processor;

import com.sum.spring_cloud_stream.sec10.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.function.Function;

@Configuration
public class ProcessorConfig {

    private static final Logger log = LoggerFactory.getLogger(ProcessorConfig.class);
    private static final String SPRING_CLOUD_STREAM_DESTINATION = "spring.cloud.stream.sendto.destination";
    private static final String DIGITAL_OUT = "digital-delivery-out";
    private static final String FEDEX_OUT = "fedex-delivery-out";
    private static final String USPS_OUT = "usps-delivery-out";
    private final CarrierAvailabilityService carrierAvailabilityService;

    public ProcessorConfig(CarrierAvailabilityService carrierAvailabilityService) {
        this.carrierAvailabilityService = carrierAvailabilityService;
    }

    @Bean
    public Function<Order, Message<?>> deliveryProcessor() {
        return this::dispatch;
    }

    private Message<?> dispatch(Order order) {
        log.info("dispatching order: {}", order);
        return switch (order.productType()) {
            case DIGITAL -> this.toMessage(this.toDigitalDelivery(order), DIGITAL_OUT);
            case PHYSICAL -> {
                var outputBinding = carrierAvailabilityService.isFedexAvailable() ? FEDEX_OUT : USPS_OUT;
                yield this.toMessage(this.toPhysicalDelivery(order), outputBinding);
            }
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

    private Message<?> toMessage(Object payload, String destination) {
        return MessageBuilder.withPayload(payload)
                             .setHeader(SPRING_CLOUD_STREAM_DESTINATION, destination)
                             .build();
    }

}