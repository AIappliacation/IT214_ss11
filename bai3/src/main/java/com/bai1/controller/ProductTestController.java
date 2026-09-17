package com.bai1.controller;

import com.bai1.client.ProductClient;
import com.bai1.dto.ProductInfo;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/test/products")
public class ProductTestController {

    private final ProductClient productClient;

    public ProductTestController(ProductClient productClient) {
        this.productClient = productClient;
    }

    @GetMapping("/{id}")
    public ProductInfo getById(@PathVariable Long id) {
        return productClient.getById(id);
    }

    @GetMapping
    public List<ProductInfo> getAll() {
        return productClient.getAll();
    }
}