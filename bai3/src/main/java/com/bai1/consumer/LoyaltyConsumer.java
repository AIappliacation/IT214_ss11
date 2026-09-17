package com.bai1.consumer;

import com.bai1.dto.OrderEvent;
import com.bai1.service.LoyaltyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class LoyaltyConsumer {

    private static final Logger log = LoggerFactory.getLogger(LoyaltyConsumer.class);

    private final LoyaltyService loyaltyService;

    public LoyaltyConsumer(LoyaltyService loyaltyService) {
        this.loyaltyService = loyaltyService;
    }

    /**
     * BÀI TẬP 3: LOYALTY-SERVICE CONSUMER
     * - Group ID: loyalty-group (Độc lập hoàn toàn với inventory-group).
     * - Cơ chế Fan-out (Publish-Subscribe): Cả Inventory-Service và Loyalty-Service
     *   đều nhận đủ 100% các sự kiện order.created từ Topic storex-order-events
     *   mà không bị tranh chấp / giành giật bản tin của nhau.
     */
    @KafkaListener(
            topics = "storex-order-events",
            groupId = "${loyalty.kafka.consumer.group-id:loyalty-group}",
            containerFactory = "loyaltyKafkaListenerContainerFactory"
    )
    public void consume(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("[LOYALTY CONSUMER] Nhận đơn hàng [Topic: {}, Partition: {}, Offset: {}] -> Tích điểm cho khách: {}",
                topic, partition, offset, event.getCustomerId());

        loyaltyService.addPoints(
                event.getOrderId(),
                event.getCustomerId(),
                event.getTotalPrice(),
                partition,
                offset
        );
    }
}
