package com.sum.spring_cloud_stream.sec13.dto;

public record Order(int id,
                    int customerId,
                    int amount,
                    ProductType productType) {
}