package com.bai1.consumer;

import com.bai1.config.KafkaTopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class DeadLetterQueueConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueConsumer.class);

    /**
     * BÀI TẬP 4: DEAD LETTER QUEUE (DLQ) LISTENER
     * - Topic: storex-order-events.DLQ
     * - GroupId: inventory-dlq-group
     * - Lắng nghe các thông điệp bị lỗi sau khi đã thử lại 3 lần thất bại.
     */
    @KafkaListener(
            topics = KafkaTopicConfig.STOREX_ORDER_EVENTS_DLQ_TOPIC,
            groupId = "inventory-dlq-group"
    )
    public void consumeDeadLetterQueue(
            @Payload Object payload,
            @Header(value = KafkaHeaders.RECEIVED_TOPIC, required = false) String topic,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partition,
            @Header(value = KafkaHeaders.OFFSET, required = false) Long offset,
            @Header(value = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) String originalTopic,
            @Header(value = KafkaHeaders.DLT_ORIGINAL_PARTITION, required = false) Integer originalPartition,
            @Header(value = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false) Long originalOffset,
            @Header(value = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage
    ) {
        log.error("==================== [DEAD LETTER QUEUE (DLQ) TIẾP NHẬN] ====================");
        log.error("Xác nhận: Đã tiếp nhận đơn hàng bị lỗi vào hộp thư chết DLQ!");
        log.error("DLQ Vị trí: [Topic: {}, Partition: {}, Offset: {}]", topic, partition, offset);
        log.error("Nguồn gốc thất bại: [Topic: {}, Partition: {}, Offset: {}]", originalTopic, originalPartition, originalOffset);
        log.error("Nguyên nhân lỗi (Exception): {}", exceptionMessage);
        log.error("Payload đơn hàng: {}", payload);
        log.error("=============================================================================");
    }
}
