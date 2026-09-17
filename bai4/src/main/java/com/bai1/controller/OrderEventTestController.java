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
     * Gửi một sự kiện đơn hàng hợp lệ vào topic "order-events"
     * Consumer sẽ nhận và khấu trừ kho thành công
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
        kafkaTemplate.send(KafkaTopicConfig.ORDER_EVENTS_TOPIC, orderEvent.getOrderId(), orderEvent);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        response.put("message", "Đã gửi đơn hàng hợp lệ vào topic " + KafkaTopicConfig.ORDER_EVENTS_TOPIC);
        response.put("data", orderEvent);
        return ResponseEntity.ok(response);
    }

    /**
     * Gửi chuỗi JSON sai định dạng (ví dụ thiếu ngoặc nhọn '}')
     * Mục đích: Kiểm thử ErrorHandlingDeserializer + DefaultErrorHandler retry 3 lần và đẩy vào DLQ (order-events.DLT)
     * Consumer KHÔNG BỊ KẸT và tiếp tục nhận các message sau.
     */
    @PostMapping("/malformed")
    public ResponseEntity<Map<String, Object>> sendMalformedJsonEvent() {
        // Chuỗi JSON hỏng: thiếu dấu '}' đóng chuỗi
        String malformedJson = "{\"orderId\":\"ORD-ERR-999\",\"productId\":\"PROD-001\",\"quantity\":5";

        log.warn("[Controller] Đang cố tình gửi JSON sai cú pháp vào topic {}: {}", 
                KafkaTopicConfig.ORDER_EVENTS_TOPIC, malformedJson);
        stringKafkaTemplate.send(KafkaTopicConfig.ORDER_EVENTS_TOPIC, "ORD-ERR-999", malformedJson);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "SENT_MALFORMED");
        response.put("message", "Đã gửi JSON hỏng cú pháp vào topic. Quan sát log: Retry 3 lần -> Chuyển sang DLQ!");
        response.put("rawPayload", malformedJson);
        return ResponseEntity.ok(response);
    }

    /**
     * Gửi đơn hàng chứa productId "ERR_RETRY" để giả lập lỗi Exception trong logic xử lý của Consumer
     * Mục đích: Kiểm thử Consumer ném Exception -> ErrorHandler retry 3 lần -> Chuyển sang DLQ
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
        kafkaTemplate.send(KafkaTopicConfig.ORDER_EVENTS_TOPIC, retryEvent.getOrderId(), retryEvent);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "SENT_RETRY_TEST");
        response.put("message", "Đã gửi đơn hàng gây lỗi. Quan sát log: Retry 3 lần -> Chuyển sang DLQ!");
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
