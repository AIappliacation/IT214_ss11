package com.bai1.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final Map<String, Integer> stockDatabase = new ConcurrentHashMap<>();
    private final AtomicInteger processedOrderCount = new AtomicInteger(0);

    public InventoryService() {
        stockDatabase.put("PROD-001", 1000);
        stockDatabase.put("PROD-002", 500);
        stockDatabase.put("PROD-003", 200);
    }

    public void deductStock(String orderId, String productId, int quantity, int partition, long offset) {
        String threadName = Thread.currentThread().getName();
        int total = processedOrderCount.incrementAndGet();

        log.info("[INVENTORY-SERVICE] [Thread: {}] Xử lý đơn hàng: {} | Sản phẩm: {} | SL: {} | [Partition: {}, Offset: {}] | Tổng đơn đã xử lý: {}",
                threadName, orderId, productId, quantity, partition, offset, total);

        int current = stockDatabase.getOrDefault(productId, 100);
        stockDatabase.put(productId, Math.max(0, current - quantity));
    }

    public int getProcessedCount() {
        return processedOrderCount.get();
    }
}
