package com.bai1.controller;

import com.bai1.dto.Banner;
import com.bai1.service.PromotionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BannerControllerTest {

    private PromotionService promotionService;
    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        promotionService = mock(PromotionService.class);
        BannerController controller = new BannerController(promotionService);
        webTestClient = WebTestClient.bindToController(controller).build();
    }

    @Test
    @DisplayName("GET /api/banners/active - Trả về HTTP 200 và Banner JSON thành công")
    void testGetActiveBannerEndpoint_Success() {
        Banner banner = new Banner(
                1L,
                "Flash Sale 12.12",
                "/images/banner12.jpg",
                "/sale-12",
                "Khuyến mãi lớn nhất",
                true
        );

        when(promotionService.getActiveBanner()).thenReturn(Mono.just(banner));

        webTestClient.get()
                .uri("/api/banners/active")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentType("application/json")
                .expectBody()
                .jsonPath("$.id").isEqualTo(1)
                .jsonPath("$.title").isEqualTo("Flash Sale 12.12")
                .jsonPath("$.imageUrl").isEqualTo("/images/banner12.jpg")
                .jsonPath("$.active").isEqualTo(true);
    }

    @Test
    @DisplayName("GET /api/banners/active - Fallback khi có sự cố trả về default banner")
    void testGetActiveBannerEndpoint_Fallback() {
        when(promotionService.getActiveBanner()).thenReturn(Mono.just(Banner.defaultBanner()));

        webTestClient.get()
                .uri("/api/banners/active")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.title").isEqualTo("Khuyến mãi đang được cập nhật")
                .jsonPath("$.id").isEqualTo(0);
    }
}
