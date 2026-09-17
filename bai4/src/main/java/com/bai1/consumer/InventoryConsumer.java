package com.bai1.consumer;

import com.bai1.dto.OrderEvent;
import com.bai1.service.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    private final InventoryService inventoryService;

    public InventoryConsumer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    /**
     * Lắng nghe đơn đặt hàng mới từ topic "order-events"
     * Khi cấu hình DefaultErrorHandler + DeadLetterPublishingRecoverer + ErrorHandlingDeserializer:
     * - Nếu message lỗi JSON cú pháp (thiếu }), deserializer bọc lỗi và chuyển cho error handler retry 3 lần rồi đưa sang DLQ.
     * - Nếu method ném RuntimeException (lỗi nghiệp vụ, db timeout), error handler retry 3 lần rồi đưa sang DLQ.
     * - Consumer không bị kẹt, partition offset được commit và chuyển sang message tiếp theo.
     */
    @KafkaListener(
            topics = "order-events",
            groupId = "inventory-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("==> [InventoryConsumer] Đã nhận tin nhắn từ [Topic: {}, Partition: {}, Offset: {}]", 
                topic, partition, offset);
        log.info("[InventoryConsumer] Dữ liệu đơn hàng: {}", event);

        // Khấu trừ tồn kho
        inventoryService.deductStock(event.getProductId(), event.getQuantity());

        log.info("<== [InventoryConsumer] Xử lý đơn hàng thành công cho OrderId: {}", event.getOrderId());
    }
}
