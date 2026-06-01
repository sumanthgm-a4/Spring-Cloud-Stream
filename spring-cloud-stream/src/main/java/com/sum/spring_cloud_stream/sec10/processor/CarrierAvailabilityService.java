package com.sum.spring_cloud_stream.sec10.processor;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class CarrierAvailabilityService {

    private final AtomicBoolean fedexAvailable = new AtomicBoolean(true);

    public boolean isFedexAvailable() {
        return fedexAvailable.get();
    }

    @Scheduled(fixedDelay = 10_000)
    void simulateAvailabilityChange() {
        // toggle
        // only one thread updates. this is safe
        fedexAvailable.set(!fedexAvailable.get());
    }

}