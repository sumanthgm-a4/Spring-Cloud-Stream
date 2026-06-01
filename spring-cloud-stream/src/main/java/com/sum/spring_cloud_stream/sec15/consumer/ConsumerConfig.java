package com.sum.spring_cloud_stream.sec15.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

@Configuration
public class ConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(ConsumerConfig.class);

    @Bean
    public Consumer<Message<String>> consumer(){
        return this::handleMessage;
    }

    private void handleMessage(Message<String> message){
        log.info("message: {}", message);

        // available only when ack-mode = MANUAL
        var acknowledgement = message.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
        if(Objects.isNull(acknowledgement)){
            throw new IllegalStateException("Acknowledgment is required in MANUAL ack mode");
        }

        switch (message.getPayload()){
            case "4", "5", "6" -> simulateNoAcknowledgement();
            case "7" -> simulateTemporaryFailure(acknowledgement);
            default -> acknowledgement.acknowledge();
        }

    }

    private void simulateNoAcknowledgement() {
        log.info("message processed but not acknowledged");
        // no ack - offset not committed
    }


    private void simulateTemporaryFailure(Acknowledgment acknowledgment){
        var random = ThreadLocalRandom.current().nextInt(1, 11);
        log.info("random value: {}", random);
        if(random > 8){
            log.info("processing succeeded");
            acknowledgment.acknowledge();
        }else {
            log.info("temporary failure, retry after 5 seconds");
            acknowledgment.nack(Duration.ofSeconds(5));
        }
    }

}