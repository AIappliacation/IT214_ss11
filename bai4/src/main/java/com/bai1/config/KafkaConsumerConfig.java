package com.bai1.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@SuppressWarnings("removal")
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:inventory-group}")
    private String groupId;

    /**
     * 1. Cấu hình ConsumerFactory với ErrorHandlingDeserializer & Trusted Packages
     * Giải quyết lỗi BUG-05:
     * Spring Kafka mặc định từ chối parse cấu trúc JSON nếu class không nằm trong danh sách tin cậy.
     * Bắt buộc cấu hình: trusted.packages=* (hoặc JsonDeserializer.TRUSTED_PACKAGES = "*")
     * để cho phép Kafka tự động giải mã Object.
     */
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Sử dụng ErrorHandlingDeserializer bọc ngoài để bẫy lỗi JSON sai định dạng
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        // Khai báo serializer ủy quyền bên trong (delegate)
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        // BUG-05 FIX: Cho phép giải mã tất cả các package đáng tin cậy
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put("spring.json.trusted.packages", "*");
        props.put("trusted.packages", "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.bai1.dto.OrderEvent");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * 2. Cấu hình DeadLetterPublishingRecoverer (Cơ chế Recovery)
     * - Nếu thử lại 3 lần vẫn thất bại, hệ thống tự động loại bỏ thông điệp lỗi đó
     *   khỏi Partition chính và đẩy sang Topic đặc biệt có tên: storex-order-events.DLQ
     * - REQ-02: In log màu đỏ / mức ERROR thông báo chính xác:
     *   "Đã ném đơn hàng bị lỗi vào DLQ" khi có ngoại lệ xảy ra sau 3 lần retry.
     */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, Object> kafkaTemplate) {
        return new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> {
            // REQ-02: Ghi nhận hành vi ném vào DLQ với mức ERROR chuẩn xác
            log.error("==================================================================================");
            log.error("Đã ném đơn hàng bị lỗi vào DLQ");
            log.error("==> Bản tin gốc: [Topic: {}, Partition: {}, Offset: {}, Key: {}]",
                    record.topic(), record.partition(), record.offset(), record.key());
            log.error("==> Nguyên nhân ngoại lệ sau 3 lần retry: {}", ex.getMessage());
            log.error("==> Đích chuyển tiếp: {}", KafkaTopicConfig.STOREX_ORDER_EVENTS_DLQ_TOPIC);
            log.error("==================================================================================");

            // Đẩy sang topic storex-order-events.DLQ
            return new TopicPartition(KafkaTopicConfig.STOREX_ORDER_EVENTS_DLQ_TOPIC, record.partition());
        });
    }

    /**
     * 3. Cấu hình DefaultErrorHandler (Cơ chế Retry)
     * - Yêu cầu: Consumer tự động thử lại (Retry) tối đa 3 lần, mỗi lần cách nhau 2 giây.
     * - FixedBackOff(2000L, 3L):
     *     + interval = 2000L (2 giây)
     *     + maxAttempts = 3L (3 lần retry)
     */
    @Bean
    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
        FixedBackOff backOff = new FixedBackOff(2000L, 3L);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(deadLetterPublishingRecoverer, backOff);

        // Log cảnh báo mỗi lần retry
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.warn("[RETRY-CONTROLLER] Thử lại lần {}/3 cho đơn hàng tại [Topic: {}, Partition: {}, Offset: {}]. Lỗi phát sinh: {}",
                    deliveryAttempt, record.topic(), record.partition(), record.offset(), ex.getMessage());
        });

        return errorHandler;
    }

    /**
     * 4. Cấu hình ContainerFactory tích hợp ErrorHandler & AckMode RECORD
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler errorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);

        // Commit offset ngay sau khi xử lý thành công hoặc sau khi đã đẩy sang DLQ an toàn
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        return factory;
    }
}
