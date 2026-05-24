package com.sum.spring_cloud_stream.sec08.dto;

public record Notification(
    int orderId,
    NotificationChannel channel,
    String recipient
) {}
