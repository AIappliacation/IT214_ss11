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
     * 1. Cấu hình ConsumerFactory với ErrorHandlingDeserializer
     * Khi JSON bị lỗi cú pháp (ví dụ thiếu ngoặc '}'), JsonDeserializer sẽ gặp ngoại lệ.
     * ErrorHandlingDeserializer sẽ "bọc" lỗi này lại và chuyển giao cho ErrorHandler
     * thay vì văng Exception trực tiếp làm sập vòng lặp poll() của Consumer!
     */
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Sử dụng ErrorHandlingDeserializer cho cả Key và Value
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        // Khai báo deserializer thực thi bên trong (delegate)
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        // Cấu hình cho JsonDeserializer
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.bai1.dto.OrderEvent");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * 2. Cấu hình DeadLetterPublishingRecoverer
     * Chịu trách nhiệm gửi message lỗi sang topic Dead Letter Queue (mặc định thêm hậu tố .DLT)
     * Ví dụ: message từ topic "order-events" sẽ được đẩy vào topic "order-events.DLT"
     */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, Object> kafkaTemplate) {
        return new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> {
            log.error("[DLQ-RECOVERER] Chuyển tiếp message thất bại sang Dead Letter Queue!");
            log.error("==> Topic nguồn: {}, Partition: {}, Offset: {}", 
                    record.topic(), record.partition(), record.offset());
            log.error("==> Nguyên nhân lỗi: {}", ex.getMessage());
            
            // Đẩy sang topic có tên <topic_gốc>.DLT
            return new TopicPartition(record.topic() + ".DLT", record.partition());
        });
    }

    /**
     * 3. Cấu hình DefaultErrorHandler với Retry và Dead Letter Queue
     * - Thử lại (Retry) tối đa 3 lần: khoảng cách giữa các lần thử lại là 1000ms (1 giây)
     * - Sau khi hết 3 lần thử lại mà vẫn lỗi -> gọi DeadLetterPublishingRecoverer đẩy vào DLQ
     * - Commit offset của message lỗi để Consumer KHÔNG BỊ KẸT và tiếp tục đọc tin nhắn kế tiếp
     */
    @Bean
    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
        // FixedBackOff(interval, maxAttempts): 
        // 1000ms interval, 3 lần retry (tổng cộng 1 lần gốc + 3 lần thử lại = 4 lượt gửi, hoặc 2 lần retry nếu tính tổng 3 attempts)
        // Đề bài yêu cầu: "Thử lại (Retry) tối đa 3 lần nếu gặp lỗi" -> interval 1000ms, retry 3 lần
        FixedBackOff backOff = new FixedBackOff(1000L, 3L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(deadLetterPublishingRecoverer, backOff);

        // Thiết lập listener để log chi tiết mỗi lần thử lại
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.warn("[RETRY-HANDLER] Đang thử lại lần thứ {}/3 cho message tại [Topic: {}, Partition: {}, Offset: {}]. Lỗi: {}",
                    deliveryAttempt, record.topic(), record.partition(), record.offset(), ex.getMessage());
        });

        return errorHandler;
    }

    /**
     * 4. Cấu hình ContainerFactory tích hợp ErrorHandler
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler errorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        // Gán ErrorHandler đã cấu hình Retry + DLQ
        factory.setCommonErrorHandler(errorHandler);

        // RECORD: Commit offset ngay sau khi record được xử lý thành công hoặc đã được DLQ recover
        // Giúp đảm bảo tính toàn vẹn và không bị kẹt offset
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        return factory;
    }
}
