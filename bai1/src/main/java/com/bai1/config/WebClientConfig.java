package com.bai1.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;

import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Configuration
public class WebClientConfig {

    @Bean
    public WebClient webClient() {

        HttpClient httpClient = HttpClient.create()
                .responseTimeout(Duration.ofSeconds(5))
                .option(
                        ChannelOption.CONNECT_TIMEOUT_MILLIS,
                        5000
                )
                .doOnConnected(connection ->
                        connection
                                .addHandlerLast(
                                        new ReadTimeoutHandler(
                                                5,
                                                TimeUnit.SECONDS
                                        )
                                )
                                .addHandlerLast(
                                        new WriteTimeoutHandler(
                                                5,
                                                TimeUnit.SECONDS
                                        )
                                )
                );

        return WebClient.builder()
                .clientConnector(
                        new ReactorClientHttpConnector(httpClient)
                )
                .build();
    }
}
