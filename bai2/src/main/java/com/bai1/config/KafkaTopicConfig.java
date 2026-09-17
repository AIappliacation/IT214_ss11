package com.bai1.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String STOREX_ORDER_EVENTS_TOPIC = "storex-order-events";

    /**
     * Topic storex-order-events được cấu hình có 5 Partitions
     * Giải quyết BUG-03: Phải cấu hình số partition phù hợp và sử dụng orderId làm Key
     * để các sự kiện cùng 1 đơn hàng luôn phân tuyến vào cùng 1 Partition.
     */
    @Bean
    public NewTopic storexOrderEventsTopic() {
        return TopicBuilder.name(STOREX_ORDER_EVENTS_TOPIC)
                .partitions(5)
                .replicas(1)
                .build();
    }
}
