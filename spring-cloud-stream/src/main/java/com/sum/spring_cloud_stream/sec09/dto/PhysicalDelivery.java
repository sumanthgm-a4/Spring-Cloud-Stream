package com.sum.spring_cloud_stream.sec09.dto;

public record PhysicalDelivery(int orderId,
                               String street,
                               String city) {
}