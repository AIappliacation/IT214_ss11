package com.bai1.controller;

import com.bai1.config.KafkaTopicConfig;
import com.bai1.dto.OrderEvent;
import com.bai1.dto.OrderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * BÀI TẬP 2: API ĐẶT HÀNG BẤT ĐỒNG BỘ (EVENT PRODUCER)
     * - Endpoint: POST /api/v1/orders
     * - Output: HTTP Status 202 Accepted kèm body chứa orderId dưới dạng Mono<String>
     * - Logic: Đẩy event "order.created" vào Kafka Topic "storex-order-events"
     * - BUG-03 Fix: Sử dụng orderId làm message Key để đảm bảo các sự kiện cùng 1 đơn hàng
     *   luôn rơi vào cùng 1 partition trong tổng số 5 partitions.
     */
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<String> createOrder(@RequestBody(required = false) OrderRequest request) {
        if (request == null) {
            request = new OrderRequest();
        }

        // Tự động sinh orderId nếu chưa có
        String orderId = (request.getOrderId() != null && !request.getOrderId().trim().isEmpty())
                ? request.getOrderId()
                : "ORD-" + UUID.randomUUID().toString().substring(0, 8);

        // Đóng gói sự kiện mang tên order.created
        OrderEvent orderEvent = new OrderEvent(
                orderId,
                request.getCustomerId() != null ? request.getCustomerId() : "CUST-001",
                request.getProductId() != null ? request.getProductId() : "PROD-001",
                request.getQuantity() != null ? request.getQuantity() : 1,
                request.getTotalPrice(),
                request.getDeliveryAddress()
        );

        log.info("[Order-Service] Nhận yêu cầu đặt hàng mới. Chuẩn bị phát event 'order.created' với OrderId: {}", orderId);

        /*
         * BUG-03 FIX:
         * Topic storex-order-events có 5 partitions.
         * Khi gọi send(), bắt buộc truyền orderId làm KEY:
         * kafkaTemplate.send(topic, KEY, PAYLOAD)
         * -> Kafka Partitioner sẽ hash key (MurmurHash3) để chọn partition cố định cho cùng orderId.
         */
        kafkaTemplate.send(KafkaTopicConfig.STOREX_ORDER_EVENTS_TOPIC, orderId, orderEvent)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("[Order-Service] [PRODUCE SUCCESS] Sự kiện 'order.created' đã gửi vào [Topic: {}, Partition: {}, Offset: {}] với Key: {}",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                orderId);
                    } else {
                        log.error("[Order-Service] [PRODUCE ERROR] Gửi sự kiện thất bại cho OrderId {}: {}", orderId, ex.getMessage());
                    }
                });

        // Trả về ngay lập tức HTTP 202 Accepted kèm body là orderId (Non-blocking)
        return Mono.just(orderId);
    }

    @GetMapping("/hello")
    public Mono<String> hello() {
        return Mono.just("Order Service (Event Producer) is running!");
    }
}
