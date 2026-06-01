package com.sum.spring_cloud_stream.sec18.dto;

public record TransferRequest(String fromAccount,
                              String toAccount,
                              Integer amount) {
}