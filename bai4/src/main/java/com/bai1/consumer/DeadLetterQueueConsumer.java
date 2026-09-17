package com.bai1.consumer;

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
     * Lắng nghe các message bị lỗi chuyển về Dead Letter Queue (Topic: order-events.DLT)
     * Sau khi retry tối đa 3 lần thất bại, message sẽ được đưa vào đây.
     * Tại đây, hệ thống có thể:
     * 1. Ghi log cảnh báo mức ERROR / ALERT cho đội ngũ vận hành.
     * 2. Lưu tin nhắn lỗi vào Database (bảng dlq_audit_log).
     * 3. Bắn thông báo qua Slack/Telegram/PagerDuty.
     * 4. Cung cấp API cho phép replay/reprocess sau khi sửa dữ liệu.
     */
    @KafkaListener(
            topics = "order-events.DLT",
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
        log.error("==================== [DEAD LETTER QUEUE (DLQ) ALERT] ====================");
        log.error("Phát hiện message thất bại sau khi đã retry tối đa 3 lần!");
        log.error("DLQ Topic: {}, Partition: {}, Offset: {}", topic, partition, offset);
        log.error("Nguyên bản - Topic: {}, Partition: {}, Offset: {}", originalTopic, originalPartition, originalOffset);
        log.error("Nguyên nhân lỗi (Exception Message): {}", exceptionMessage);
        log.error("Nội dung payload bị lỗi: {}", payload);
        log.error("=========================================================================");
    }
}
