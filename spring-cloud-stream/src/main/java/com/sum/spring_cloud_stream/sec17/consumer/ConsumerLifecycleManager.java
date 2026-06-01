package com.sum.spring_cloud_stream.sec17.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class ConsumerLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(ConsumerLifecycleManager.class);
    private static final String CONSUMER_CHANNEL = "consumer-in-0";
    private final BindingsLifecycleController bindingService;

    public ConsumerLifecycleManager(BindingsLifecycleController bindingService) {
        this.bindingService = bindingService;
    }

    public void pause() {
        this.bindingService.changeState(CONSUMER_CHANNEL, BindingsLifecycleController.State.PAUSED);
    }

    public void resume() {
        this.bindingService.changeState(CONSUMER_CHANNEL, BindingsLifecycleController.State.RESUMED);
    }

    // this is just for demo purposes
    @Scheduled(fixedRate = 10_000, initialDelay = 10_000)
    public void checkDependenciesHealth() {
        var isPaused = this.bindingService.queryState(CONSUMER_CHANNEL)
                                          .stream()
                                          .allMatch(Binding::isPaused);
        if (isPaused) { // paused => resume
            log.info("resuming the consumer");
            this.resume();
        } else { // running => pause
            log.info("pausing the consumer");
            this.pause();
        }
    }

}