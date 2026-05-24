package com.sum.spring_cloud_stream.sec07.dto;

import java.util.UUID;

public record Payment(
    int orderId,
    int amount,
    UUID paymentId
) {}
