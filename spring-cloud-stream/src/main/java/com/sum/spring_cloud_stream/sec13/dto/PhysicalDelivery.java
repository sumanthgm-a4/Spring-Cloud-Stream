package com.sum.spring_cloud_stream.sec13.dto;

public record PhysicalDelivery(int orderId,
                               String street,
                               String city) {
}