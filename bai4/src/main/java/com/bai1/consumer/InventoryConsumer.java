package com.bai1.consumer;

import com.bai1.config.KafkaTopicConfig;
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
     * BÀI TẬP 4: LẮNG NGHE ĐƠN ĐẶT HÀNG TẠI INVENTORY-SERVICE
     * - Topic: storex-order-events
     * - GroupId: inventory-group
     * - Kịch bản lỗi:
     *   1. Kiện hàng bị lỗi định dạng JSON (thiếu dấu '}') -> ErrorHandlingDeserializer bẫy lỗi.
     *   2. Đơn hàng chứa mã sản phẩm không tồn tại (productId: null) -> ném ngoại lệ IllegalArgumentException.
     * - Khi ném Exception: DefaultErrorHandler sẽ retry tối đa 3 lần, mỗi lần cách nhau 2 giây.
     * - Nếu sau 3 lần vẫn lỗi -> Tự động loại bỏ khỏi partition chính và đẩy sang storex-order-events.DLQ.
     */
    @KafkaListener(
            topics = KafkaTopicConfig.STOREX_ORDER_EVENTS_TOPIC,
            groupId = "inventory-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("==> [InventoryConsumer] Đã nhận đơn hàng từ [Topic: {}, Partition: {}, Offset: {}]", 
                topic, partition, offset);
        log.info("[InventoryConsumer] Dữ liệu đơn hàng: {}", event);

        // Kịch bản đề bài: Đơn hàng chứa mã sản phẩm không tồn tại (productId: null)
        if (event.getProductId() == null || event.getProductId().trim().isEmpty()) {
            log.error("[InventoryConsumer] [LỖI NGHIỆP VỤ] Đơn hàng {} có productId: null! Ném ngoại lệ để kích hoạt Retry + DLQ.",
                    event.getOrderId());
            throw new IllegalArgumentException("Mã sản phẩm không tồn tại (productId: null) trong đơn hàng " + event.getOrderId());
        }

        // Khấu trừ tồn kho
        inventoryService.deductStock(event.getProductId(), event.getQuantity());

        log.info("<== [InventoryConsumer] Đã trừ kho thành công cho OrderId: {}", event.getOrderId());
    }
}
