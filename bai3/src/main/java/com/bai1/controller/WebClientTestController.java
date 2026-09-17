package com.bai1.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;

import reactor.core.publisher.Mono;

@RestController
public class WebClientTestController {

    private final WebClient webClient;

    public WebClientTestController(WebClient webClient) {
        this.webClient = webClient;
    }

    @GetMapping("/api/orders/test-client")
    public Mono<String> testClient() {

        return webClient
                .get()
                .uri("http://localhost:9999/test")
                .retrieve()
                .bodyToMono(String.class)
                .timeout(java.time.Duration.ofSeconds(5))
                .onErrorReturn("Request timeout or service unavailable");
    }
}
