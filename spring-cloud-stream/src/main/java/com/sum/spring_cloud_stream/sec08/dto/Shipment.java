package com.sum.spring_cloud_stream.sec08.dto;

public record Shipment(
    int orderId,
    String trackingId
) {}
