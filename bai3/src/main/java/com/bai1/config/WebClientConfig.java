package com.bai1.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Configuration
public class WebClientConfig {

    @Value("${promotion.service.url:http://promotion-service}")
    private String promotionServiceBaseUrl;

    /**
     * Cấu hình Netty HttpClient tối ưu cho môi trường chịu tải cao (Flash Sale 10.000 concurrent requests).
     * - ConnectionProvider với pool size lớn (maxConnections = 1000, pendingAcquireMaxCount = 10000)
     * - Connect timeout: 2 giây
     * - Response timeout: 2 giây
     * - Read/Write timeout: 2 giây
     */
    @Bean
    public HttpClient httpClient() {
        ConnectionProvider connectionProvider = ConnectionProvider.builder("custom-connection-pool")
                .maxConnections(1000)
                .pendingAcquireMaxCount(10000)
                .pendingAcquireTimeout(Duration.ofSeconds(2))
                .maxIdleTime(Duration.ofSeconds(20))
                .build();

        return HttpClient.create(connectionProvider)
                .responseTimeout(Duration.ofSeconds(2))
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
                .doOnConnected(connection ->
                        connection
                                .addHandlerLast(new ReadTimeoutHandler(2, TimeUnit.SECONDS))
                                .addHandlerLast(new WriteTimeoutHandler(2, TimeUnit.SECONDS))
                );
    }

    /**
     * Bean WebClient chung cho toàn hệ thống
     */
    @Bean
    @Primary
    public WebClient webClient(HttpClient httpClient) {
        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }

    /**
     * WebClient chuyên biệt cho Promotion Service với baseUrl đã cấu hình
     */
    @Bean(name = "promotionWebClient")
    public WebClient promotionWebClient(HttpClient httpClient) {
        return WebClient.builder()
                .baseUrl(promotionServiceBaseUrl)
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }
}
