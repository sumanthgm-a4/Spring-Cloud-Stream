package com.sum.spring_cloud_stream.sec04.producer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Configuration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class PingProducer implements CommandLineRunner {

    private final StreamBridge streamBridge;

    private static final String PING_OUT = "ping-out";

    @Override
    public void run(String... args) throws Exception {
        // Ping google.com 
        var process = new ProcessBuilder("ping", "-c", "15", "google.com")
            .redirectErrorStream(true)
            .start();

        try (var reader = process.inputReader()) {
            reader.lines()
                .forEach(line -> {
                    log.info("SENDING: {}", line);
                    streamBridge.send(PING_OUT, line);
                });
        }
    }
}
