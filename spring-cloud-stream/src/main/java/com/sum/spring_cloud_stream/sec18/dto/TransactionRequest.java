package com.sum.spring_cloud_stream.sec18.dto;

public record TransactionRequest(String account,
                                 Integer amount,
                                 TransactionType type) {
}