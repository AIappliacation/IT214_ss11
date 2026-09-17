package com.bai1.service;

import com.bai1.dto.Banner;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PromotionServiceTest {

    @Test
    @DisplayName("Thành công: Promotion Service phản hồi bình thường trả về đúng Banner")
    void testGetActiveBanner_Success() {
        // Given
        String responseJson = "{"
                + "\"id\": 100,"
                + "\"title\": \"Siêu Sale 11.11 - Giảm 50%\","
                + "\"imageUrl\": \"https://storex.vn/banners/sale11.png\","
                + "\"targetUrl\": \"https://storex.vn/events/11-11\","
                + "\"description\": \"Cơ hội mua sắm giảm giá lớn nhất năm\","
                + "\"active\": true"
                + "}";

        ExchangeFunction exchangeFunction = mock(ExchangeFunction.class);
        when(exchangeFunction.exchange(any())).thenReturn(
                Mono.just(ClientResponse.create(HttpStatus.OK)
                        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .body(responseJson)
                        .build())
        );

        WebClient webClient = WebClient.builder().exchangeFunction(exchangeFunction).build();
        PromotionService service = new PromotionService(webClient);

        // When & Then
        StepVerifier.create(service.getActiveBanner())
                .assertNext(banner -> {
                    assertNotNull(banner);
                    assertEquals(100L, banner.getId());
                    assertEquals("Siêu Sale 11.11 - Giảm 50%", banner.getTitle());
                    assertEquals("https://storex.vn/banners/sale11.png", banner.getImageUrl());
                    assertTrue(banner.isActive());
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Timeout 2s: Promotion Service phản hồi chậm (> 2s) -> Fallback trả về Banner mặc định")
    void testGetActiveBanner_TimeoutFallback() {
        // Given: Giả lập Promotion Service bị chậm 3 giây (vượt ngưỡng timeout 2s)
        ExchangeFunction exchangeFunction = mock(ExchangeFunction.class);
        when(exchangeFunction.exchange(any())).thenReturn(
                Mono.delay(Duration.ofSeconds(3))
                        .map(tick -> ClientResponse.create(HttpStatus.OK)
                                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                                .body("{\"title\": \"Late response\"}")
                                .build())
        );

        WebClient webClient = WebClient.builder().exchangeFunction(exchangeFunction).build();
        PromotionService service = new PromotionService(webClient);

        // When & Then: Sử dụng StepVerifier.withVirtualTime để tua nhanh thời gian
        StepVerifier.withVirtualTime(() -> service.getActiveBanner())
                .thenAwait(Duration.ofSeconds(3))
                .assertNext(banner -> {
                    assertNotNull(banner);
                    assertEquals("Khuyến mãi đang được cập nhật", banner.getTitle());
                    assertEquals("Chương trình khuyến mãi đang được cập nhật. Vui lòng quay lại sau!", banner.getDescription());
                    assertTrue(banner.isActive());
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Service Unavailable (500 Error): Hệ thống không crash mà fallback về Banner mặc định")
    void testGetActiveBanner_ServerErrorFallback() {
        // Given: Giả lập Promotion Service trả về 500 Internal Server Error
        ExchangeFunction exchangeFunction = mock(ExchangeFunction.class);
        when(exchangeFunction.exchange(any())).thenReturn(
                Mono.just(ClientResponse.create(HttpStatus.INTERNAL_SERVER_ERROR)
                        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .body("Internal Server Error")
                        .build())
        );

        WebClient webClient = WebClient.builder().exchangeFunction(exchangeFunction).build();
        PromotionService service = new PromotionService(webClient);

        // When & Then
        StepVerifier.create(service.getActiveBanner())
                .assertNext(banner -> {
                    assertNotNull(banner);
                    assertEquals("Khuyến mãi đang được cập nhật", banner.getTitle());
                    assertEquals(0L, banner.getId());
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Network Error: Promotion Service sập hoàn toàn -> Fallback về Banner mặc định")
    void testGetActiveBanner_NetworkErrorFallback() {
        // Given: Giả lập lỗi kết nối mạng (Connection Refused)
        ExchangeFunction exchangeFunction = mock(ExchangeFunction.class);
        when(exchangeFunction.exchange(any())).thenReturn(
                Mono.error(new RuntimeException("Connection refused to http://promotion-service"))
        );

        WebClient webClient = WebClient.builder().exchangeFunction(exchangeFunction).build();
        PromotionService service = new PromotionService(webClient);

        // When & Then
        StepVerifier.create(service.getActiveBanner())
                .assertNext(banner -> {
                    assertNotNull(banner);
                    assertEquals("Khuyến mãi đang được cập nhật", banner.getTitle());
                    assertTrue(banner.isActive());
                })
                .verifyComplete();
    }
}
