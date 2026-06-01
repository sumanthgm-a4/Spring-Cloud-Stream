package com.sum.spring_cloud_stream.sec16.exception;

public class ServiceUnavailableException extends RuntimeException {

    private static final String MESSAGE = "Service unavailable";

    public ServiceUnavailableException() {
        super(MESSAGE);
    }

}
