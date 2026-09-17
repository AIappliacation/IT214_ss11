package com.bai1.controller;

import com.bai1.dto.Banner;
import com.bai1.service.PromotionService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Controller cung cấp API lấy banner trang chủ cho người dùng.
 * Hỗ trợ tải cao 10.000 concurrent users với Spring WebFlux Non-blocking.
 */
@RestController
@RequestMapping("/api/banners")
public class BannerController {

    private final PromotionService promotionService;

    public BannerController(PromotionService promotionService) {
        this.promotionService = promotionService;
    }

    /**
     * API hiển thị banner khuyến mãi ở trang chủ.
     * Non-blocking, trả về Mono<Banner>.
     */
    @GetMapping("/active")
    public Mono<Banner> getActiveBanner() {
        return promotionService.getActiveBanner();
    }
}
