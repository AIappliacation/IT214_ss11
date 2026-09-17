package com.bai1.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String STOREX_ORDER_EVENTS_TOPIC = "storex-order-events";

    /**
     * Topic storex-order-events cấu hình 5 partitions.
     * REQ-01: Khi scale 3 instances của Inventory-Service,
     * số lượng partition tối thiểu phải là 3 (Partitions >= Consumer Instances).
     * Với 5 Partitions, 3 instances sẽ tự động chia đều (2-2-1) mà không instance nào bị bỏ trống.
     */
    @Bean
    public NewTopic storexOrderEventsTopic() {
        return TopicBuilder.name(STOREX_ORDER_EVENTS_TOPIC)
                .partitions(5)
                .replicas(1)
                .build();
    }
}
