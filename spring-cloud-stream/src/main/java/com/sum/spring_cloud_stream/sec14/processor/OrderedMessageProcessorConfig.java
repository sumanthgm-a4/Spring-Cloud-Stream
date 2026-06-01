package com.sum.spring_cloud_stream.sec14.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import com.sum.spring_cloud_stream.sec14.dto.*;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Gatherers;
import java.util.stream.Stream;

@Configuration
@ConditionalOnProperty(
        name = "processing.mode",
        havingValue = "ordered"
)
public class OrderedMessageProcessorConfig {

    private static final Logger log = LoggerFactory.getLogger(OrderedMessageProcessorConfig.class);
    private static final String SPRING_CLOUD_STREAM_DESTINATION = "spring.cloud.stream.sendto.destination";
    private static final String DIGITAL_OUT = "digital-delivery-out";
    private static final String PHYSICAL_OUT = "physical-delivery-out";
    private static final int MAX_CONCURRENCY = 500;

    @Bean
    public Function<List<Order>, List<Message<Object>>> deliveryProcessor(DeliveryService deliveryService) {
        return orders -> this.partitionByCustomerId(orders)
            .gather(Gatherers.mapConcurrent(MAX_CONCURRENCY, bucket -> bucket.stream().map(deliveryService::toDelivery).toList()))
            .flatMap(List::stream)
            .map(this::toMessage)
            .toList();
    }

    // It is an example. We can also partition by message key, order id etc.
    private Stream<List<Order>> partitionByCustomerId(List<Order> orders) {
        return orders.stream()
                     .collect(
                        Collectors.groupingBy(
                            Order::customerId,
                            Collectors.toList()
                        )
                    )
                     .values()
                     .stream();
    }

    private Message<Object> toMessage(Object payload) {
        log.info("dispatching: {}", payload);
        var destination = (payload instanceof DigitalDelivery) ? DIGITAL_OUT : PHYSICAL_OUT;
        return MessageBuilder.withPayload(payload)
                             .setHeader(SPRING_CLOUD_STREAM_DESTINATION, destination)
                             .build();
    }

}