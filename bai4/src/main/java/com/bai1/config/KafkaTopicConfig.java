package com.bai1.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String ORDER_EVENTS_TOPIC = "order-events";
    public static final String ORDER_EVENTS_DLT_TOPIC = "order-events.DLT";

    /**
     * Topic chính nhận sự kiện đặt hàng từ Order Service
     */
    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(ORDER_EVENTS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Topic Dead Letter Queue (DLQ) chứa các message lỗi sau khi retry thất bại
     */
    @Bean
    public NewTopic orderEventsDltTopic() {
        return TopicBuilder.name(ORDER_EVENTS_DLT_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
