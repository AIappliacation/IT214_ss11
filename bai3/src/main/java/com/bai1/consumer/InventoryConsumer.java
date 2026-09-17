package com.bai1.consumer;

import com.bai1.dto.OrderEvent;
import com.bai1.service.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    private final InventoryService inventoryService;

    public InventoryConsumer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    /**
     * BÀI TẬP 3: INVENTORY-SERVICE CONSUMER
     * - Group ID: inventory-group (Độc lập với Loyalty-Service để thực hiện Fan-out, sửa lỗi BUG-04).
     * - Concurrency = 3: Mô phỏng 3 instance của Inventory-Service.
     *   Kafka sẽ gán 5 partition của topic storex-order-events cho 3 worker này (ví dụ 2-2-1).
     *   Mỗi instance tự động gánh ~33% tổng lượng đơn hàng, giải quyết bài toán Mega Sale 10,000 đơn/giây!
     */
    @KafkaListener(
            topics = "storex-order-events",
            groupId = "${inventory.kafka.consumer.group-id:inventory-group}",
            containerFactory = "inventoryKafkaListenerContainerFactory"
    )
    public void consume(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("[INVENTORY CONSUMER] Nhận đơn hàng [Topic: {}, Partition: {}, Offset: {}] -> Mã đơn: {}",
                topic, partition, offset, event.getOrderId());

        inventoryService.deductStock(
                event.getOrderId(),
                event.getProductId(),
                event.getQuantity() != null ? event.getQuantity() : 1,
                partition,
                offset
        );
    }
}
