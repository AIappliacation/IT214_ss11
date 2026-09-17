package com.bai1.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class OrderController {

    @GetMapping("/api/orders/hello")
    public Mono<String> hello() {
        return Mono.just("Order Service is running!");
    }
}
