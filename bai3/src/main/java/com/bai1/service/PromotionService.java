package com.bai1.service;

import com.bai1.dto.Banner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Service lấy thông tin banner khuyến mãi hiện hành từ Promotion Service.
 * Đã được chuyển đổi từ RestTemplate (Blocking) sang WebClient (Non-blocking Reactive).
 */
@Service
public class PromotionService {

    private static final Logger log = LoggerFactory.getLogger(PromotionService.class);
    private static final Duration TIMEOUT_DURATION = Duration.ofSeconds(2);
    private static final String BANNER_URI = "/api/banners/active";

    private final WebClient webClient;

    public PromotionService(@Qualifier("promotionWebClient") WebClient webClient) {
        this.webClient = webClient;
    }

    /**
     * Lấy banner khuyến mãi đang active theo cơ chế Non-blocking Reactive.
     *
     * Cải tiến so với bản cũ:
     * 1. Chuẩn Reactive: Trả về Mono<Banner> thay vì chặn thread chờ POJO.
     * 2. Non-blocking: Sử dụng WebClient của Spring WebFlux, không chiếm giữ Event Loop thread.
     * 3. Timeout 2 giây: Sử dụng toán tử .timeout(2s).
     * 4. Fallback (Resilience): Nếu timeout hoặc service chết, bắt lỗi qua onErrorResume
     *    và trả về Banner mặc định với thông báo "Khuyến mãi đang được cập nhật", không làm crash hệ thống.
     *
     * @return Mono<Banner>
     */
    public Mono<Banner> getActiveBanner() {
        return webClient.get()
                .uri(BANNER_URI)
                .retrieve()
                .bodyToMono(Banner.class)
                .timeout(TIMEOUT_DURATION)
                .doOnSuccess(banner -> {
                    if (banner != null) {
                        log.info("Lấy banner thành công từ Promotion Service: {}", banner.getTitle());
                    }
                })
                .onErrorResume(throwable -> {
                    log.warn("Gọi Promotion Service thất bại hoặc quá hạn 2s: [{}]. Kích hoạt fallback banner mặc định.",
                            throwable.getMessage());
                    // Fallback trả về Banner mặc định mà không làm crash hệ thống
                    return Mono.just(Banner.defaultBanner());
                })
                .defaultIfEmpty(Banner.defaultBanner());
    }
}
