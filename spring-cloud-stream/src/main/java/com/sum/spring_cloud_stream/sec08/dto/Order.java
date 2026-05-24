package com.sum.spring_cloud_stream.sec08.dto;

public record Order(
    int id,
    int customerId,
    int amount,
    ProductType productType
) {}
