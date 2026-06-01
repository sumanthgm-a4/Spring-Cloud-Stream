package com.sum.spring_cloud_stream.sec18.processor;

import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import com.sum.spring_cloud_stream.sec18.dto.*;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

@Configuration
public class ProcessorConfig {

    private static final String TRANSACTION_REQUEST_OUT = "transaction-request-out";
    private final StreamBridge streamBridge;

    public ProcessorConfig(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }

    @Bean
    public Consumer<Message<TransferRequest>> processor() {
        return message -> {
            var messageKey = message.getHeaders().get(KafkaHeaders.RECEIVED_KEY, String.class);
            switch (messageKey) {
                case "1" -> this.handleTransferRequest(message.getPayload(), noop(), noop()); // success
                case "2" -> this.handleTransferRequest(message.getPayload(), noop(), simulateFailure(100)); // always fails
                case "3" -> this.handleTransferRequest(message.getPayload(), simulateFailure(30), simulateFailure(30));
                case null, default -> throw new IllegalArgumentException("invalid key");
            };
        };
    }

    private void handleTransferRequest(TransferRequest request, Runnable runnable1, Runnable runnable2){
        var creditRequest = new TransactionRequest(request.toAccount(), request.amount(), TransactionType.CREDIT);
        var debitRequest = new TransactionRequest(request.fromAccount(), request.amount(), TransactionType.DEBIT);

        // emit credit request
        this.streamBridge.send(TRANSACTION_REQUEST_OUT, creditRequest);
        this.runWithDelay(runnable1);

        // emit debit request
        this.streamBridge.send(TRANSACTION_REQUEST_OUT, debitRequest);
        this.runWithDelay(runnable2);
    }

    private static Runnable noop() {
        return () -> {};
    }

    private static Runnable simulateFailure(int failurePercentage) {
        return () -> {
            if(ThreadLocalRandom.current().nextInt(1, 101) <= failurePercentage){
                throw new RuntimeException("oops");
            }
        };
    }

    private void runWithDelay(Runnable runnable){
        try {
            Thread.sleep(Duration.ofSeconds(3));
            runnable.run();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}