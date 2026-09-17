package com.bai1.controller;

import com.bai1.config.KafkaTopicConfig;
import com.bai1.dto.OrderEvent;
import com.bai1.service.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderEventTestController {

    private static final Logger log = LoggerFactory.getLogger(OrderEventTestController.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTemplate<String, String> stringKafkaTemplate;
    private final InventoryService inventoryService;

    public OrderEventTestController(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Qualifier("stringKafkaTemplate") KafkaTemplate<String, String> stringKafkaTemplate,
            InventoryService inventoryService
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.stringKafkaTemplate = stringKafkaTemplate;
        this.inventoryService = inventoryService;
    }

    /**
     * Test 1: Gửi đơn hàng hợp lệ vào topic storex-order-events
     * Kết quả mong muốn: Consumer nhận, trừ kho thành công, commit offset bình thường.
     */
    @PostMapping("/event")
    public ResponseEntity<Map<String, Object>> sendValidOrderEvent(@RequestBody(required = false) OrderEvent orderEvent) {
        if (orderEvent == null) {
            orderEvent = new OrderEvent(
                    "ORD-" + UUID.randomUUID().toString().substring(0, 8),
                    "PROD-001",
                    2,
                    new BigDecimal("150.00"),
                    "CUST-001"
            );
        }

        log.info("[Controller] Gửi đơn hàng hợp lệ: {}", orderEvent);
        kafkaTemplate.send(KafkaTopicConfig.STOREX_ORDER_EVENTS_TOPIC, orderEvent.getOrderId(), orderEvent);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        response.put("message", "Đã gửi đơn hàng hợp lệ vào topic " + KafkaTopicConfig.STOREX_ORDER_EVENTS_TOPIC);
        response.put("data", orderEvent);
        return ResponseEntity.ok(response);
    }

    /**
     * Test 2 (Đề bài): Gửi chuỗi JSON sai định dạng (thiếu dấu '}')
     * Kết quả mong muốn:
     * - ErrorHandlingDeserializer bắt lỗi cú pháp.
     * - DefaultErrorHandler retry 3 lần, mỗi lần cách nhau 2 giây.
     * - Sau 3 lần thất bại, DeadLetterPublishingRecoverer in log:
     *   "Đã ném đơn hàng bị lỗi vào DLQ"
     * - Đẩy bản tin sang topic storex-order-events.DLQ, Consumer không bị kẹt!
     */
    @PostMapping("/malformed")
    public ResponseEntity<Map<String, Object>> sendMalformedJsonEvent() {
        String malformedJson = "{\"orderId\":\"ORD-ERR-JSON\",\"productId\":\"PROD-001\",\"quantity\":5";

        log.warn("[Controller] Đang gửi JSON hỏng cú pháp (thiếu '}') vào topic {}: {}", 
                KafkaTopicConfig.STOREX_ORDER_EVENTS_TOPIC, malformedJson);
        stringKafkaTemplate.send(KafkaTopicConfig.STOREX_ORDER_EVENTS_TOPIC, "ORD-ERR-JSON", malformedJson);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "SENT_MALFORMED_JSON");
        response.put("message", "Đã gửi JSON hỏng. Quan sát log: Retry 3 lần (cách nhau 2s) -> In log ERROR 'Đã ném đơn hàng bị lỗi vào DLQ' -> Đẩy sang storex-order-events.DLQ!");
        response.put("rawPayload", malformedJson);
        return ResponseEntity.ok(response);
    }

    /**
     * Test 3 (Đề bài): Gửi đơn hàng chứa mã sản phẩm không tồn tại (productId: null)
     * Kết quả mong muốn:
     * - InventoryConsumer ném IllegalArgumentException do productId = null.
     * - DefaultErrorHandler retry 3 lần, mỗi lần cách nhau 2 giây.
     * - Sau 3 lần thất bại, in log ERROR: "Đã ném đơn hàng bị lỗi vào DLQ".
     * - Đẩy bản tin sang topic storex-order-events.DLQ.
     */
    @PostMapping("/null-product")
    public ResponseEntity<Map<String, Object>> sendNullProductEvent() {
        OrderEvent nullProductOrder = new OrderEvent(
                "ORD-NULL-" + UUID.randomUUID().toString().substring(0, 8),
                null, // productId = null theo đúng kịch bản đề bài
                2,
                new BigDecimal("300.00"),
                "CUST-888"
        );

        log.warn("[Controller] Đang gửi đơn hàng có productId: null vào topic {}: {}",
                KafkaTopicConfig.STOREX_ORDER_EVENTS_TOPIC, nullProductOrder);
        kafkaTemplate.send(KafkaTopicConfig.STOREX_ORDER_EVENTS_TOPIC, nullProductOrder.getOrderId(), nullProductOrder);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "SENT_NULL_PRODUCT_ORDER");
        response.put("message", "Đã gửi đơn hàng có productId: null. Quan sát log: Retry 3 lần (cách nhau 2s) -> Ném sang storex-order-events.DLQ!");
        response.put("data", nullProductOrder);
        return ResponseEntity.ok(response);
    }

    /**
     * Test 4: Giả lập lỗi chập chờn cơ sở dữ liệu (ERR_RETRY)
     */
    @PostMapping("/retry-test")
    public ResponseEntity<Map<String, Object>> sendRetryTestEvent() {
        OrderEvent retryEvent = new OrderEvent(
                "ORD-RETRY-" + UUID.randomUUID().toString().substring(0, 8),
                "ERR_RETRY",
                1,
                new BigDecimal("99.99"),
                "CUST-999"
        );

        log.info("[Controller] Gửi đơn hàng gây lỗi retry: {}", retryEvent);
        kafkaTemplate.send(KafkaTopicConfig.STOREX_ORDER_EVENTS_TOPIC, retryEvent.getOrderId(), retryEvent);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "SENT_RETRY_TEST");
        response.put("message", "Đã gửi đơn hàng gây lỗi. Quan sát log: Retry 3 lần (cách nhau 2s) -> Ném sang storex-order-events.DLQ!");
        response.put("data", retryEvent);
        return ResponseEntity.ok(response);
    }

    /**
     * Kiểm tra số lượng tồn kho hiện tại
     */
    @GetMapping("/stock/{productId}")
    public ResponseEntity<Map<String, Object>> getStock(@PathVariable String productId) {
        int currentStock = inventoryService.getStock(productId);
        Map<String, Object> response = new HashMap<>();
        response.put("productId", productId);
        response.put("currentStock", currentStock);
        return ResponseEntity.ok(response);
    }
}
