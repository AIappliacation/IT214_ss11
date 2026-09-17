package com.bai1.controller;

import com.bai1.config.KafkaTopicConfig;
import com.bai1.dto.OrderEvent;
import com.bai1.service.InventoryService;
import com.bai1.service.LoyaltyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final InventoryService inventoryService;
    private final LoyaltyService loyaltyService;

    public OrderController(
            KafkaTemplate<String, Object> kafkaTemplate,
            InventoryService inventoryService,
            LoyaltyService loyaltyService
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.inventoryService = inventoryService;
        this.loyaltyService = loyaltyService;
    }

    /**
     * Bắn giả lập 1 đơn hàng vào Topic storex-order-events
     * Cả Inventory-Service (inventory-group) và Loyalty-Service (loyalty-group) đều nhận được (Fan-out).
     */
    @PostMapping("/event")
    public Mono<ResponseEntity<Map<String, Object>>> sendOrderEvent(@RequestBody(required = false) OrderEvent event) {
        if (event == null) {
            String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8);
            event = new OrderEvent(
                    orderId,
                    "CUST-" + UUID.randomUUID().toString().substring(0, 4),
                    "PROD-001",
                    2,
                    new BigDecimal("200.00"),
                    "Hà Nội"
            );
        }

        OrderEvent finalEvent = event;
        kafkaTemplate.send(KafkaTopicConfig.STOREX_ORDER_EVENTS_TOPIC, finalEvent.getOrderId(), finalEvent);

        Map<String, Object> res = new HashMap<>();
        res.put("status", "SUCCESS");
        res.put("message", "Đã bắn sự kiện vào topic " + KafkaTopicConfig.STOREX_ORDER_EVENTS_TOPIC);
        res.put("orderId", finalEvent.getOrderId());
        return Mono.just(ResponseEntity.ok(res));
    }

    /**
     * Bắn giả lập số lượng lớn (ví dụ count=30) để quan sát:
     * - Cả Inventory và Loyalty đều nhận 100% dữ liệu (Fan-out).
     * - 3 instance threads của Inventory tự động chia nhau tải (~33% mỗi instance).
     */
    @PostMapping("/simulate-batch/{count}")
    public Mono<ResponseEntity<Map<String, Object>>> simulateBatch(@PathVariable int count) {
        int safeCount = Math.min(count, 100);
        log.info("[SIMULATION] Bắt đầu đẩy lô {} đơn hàng vào Kafka...", safeCount);

        for (int i = 1; i <= safeCount; i++) {
            String orderId = "ORD-BATCH-" + UUID.randomUUID().toString().substring(0, 8);
            OrderEvent event = new OrderEvent(
                    orderId,
                    "CUST-" + (1000 + i),
                    "PROD-" + ((i % 3) + 1),
                    1,
                    new BigDecimal("100.00"),
                    "Địa chỉ " + i
            );
            kafkaTemplate.send(KafkaTopicConfig.STOREX_ORDER_EVENTS_TOPIC, orderId, event);
        }

        Map<String, Object> res = new HashMap<>();
        res.put("status", "BATCH_SENT");
        res.put("totalOrdersSent", safeCount);
        res.put("message", "Đã gửi " + safeCount + " đơn hàng. Kiểm tra log console để thấy 3 thread chia tải!");
        return Mono.just(ResponseEntity.ok(res));
    }

    /**
     * Kiểm tra thống kê số lượng bản tin nhận được của từng Service
     */
    @GetMapping("/stats")
    public Mono<ResponseEntity<Map<String, Object>>> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("inventoryProcessedCount", inventoryService.getProcessedCount());
        stats.put("loyaltyProcessedCount", loyaltyService.getProcessedCount());
        stats.put("note", "Nếu hai số bằng nhau, cơ chế Fan-out 100% đã hoạt động thành công!");
        return Mono.just(ResponseEntity.ok(stats));
    }

    @GetMapping("/hello")
    public Mono<String> hello() {
        return Mono.just("Bài tập 3 - Consumer Fan-out & Partitioning is running!");
    }
}
