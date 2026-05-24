package com.sum.spring_cloud_stream.sec07.dto;

public record Shipment(
    int orderId,
    String trackingId
) {}
