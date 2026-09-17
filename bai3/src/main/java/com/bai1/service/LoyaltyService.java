package com.bai1.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class LoyaltyService {

    private static final Logger log = LoggerFactory.getLogger(LoyaltyService.class);

    private final Map<String, Integer> pointsDatabase = new ConcurrentHashMap<>();
    private final AtomicInteger processedLoyaltyCount = new AtomicInteger(0);

    public void addPoints(String orderId, String customerId, BigDecimal totalPrice, int partition, long offset) {
        String threadName = Thread.currentThread().getName();
        int total = processedLoyaltyCount.incrementAndGet();

        // Giả sử cứ 10 đơn vị tiền được 1 điểm thưởng
        int points = totalPrice != null ? totalPrice.intValue() / 10 : 10;
        int currentPoints = pointsDatabase.getOrDefault(customerId, 0) + points;
        pointsDatabase.put(customerId, currentPoints);

        log.info("[LOYALTY-SERVICE] [Thread: {}] Tích điểm cho đơn: {} | Khách hàng: {} | +{} điểm (Tổng: {}) | [Partition: {}, Offset: {}] | Tổng đơn tích điểm: {}",
                threadName, orderId, customerId, points, currentPoints, partition, offset, total);
    }

    public int getProcessedCount() {
        return processedLoyaltyCount.get();
    }
}
