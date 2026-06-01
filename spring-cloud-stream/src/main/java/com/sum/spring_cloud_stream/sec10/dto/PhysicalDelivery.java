package com.sum.spring_cloud_stream.sec10.dto;

public record PhysicalDelivery(int orderId,
                               String street,
                               String city) {
}