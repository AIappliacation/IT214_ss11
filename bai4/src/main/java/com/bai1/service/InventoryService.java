package com.bai1.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    // Mock lưu trữ tồn kho của các sản phẩm
    private final Map<String, Integer> stockDatabase = new ConcurrentHashMap<>();

    public InventoryService() {
        stockDatabase.put("PROD-001", 100);
        stockDatabase.put("PROD-002", 50);
        stockDatabase.put("PROD-003", 20);
    }

    /**
     * Khấu trừ tồn kho cho sản phẩm khi có đơn đặt hàng mới
     *
     * @param productId mã sản phẩm
     * @param quantity  số lượng cần trừ
     */
    public void deductStock(String productId, int quantity) {
        log.info("[InventoryService] Bắt đầu khấu trừ tồn kho cho sản phẩm: {}, số lượng: {}", productId, quantity);

        if (productId == null || productId.trim().isEmpty()) {
            throw new IllegalArgumentException("ProductId không được rỗng!");
        }

        if (quantity <= 0) {
            throw new IllegalArgumentException("Số lượng đặt hàng phải lớn hơn 0! Nhận được: " + quantity);
        }

        // Mô phỏng trường hợp xảy ra lỗi hệ thống / lỗi kết nối database để test retry
        if ("ERR_RETRY".equalsIgnoreCase(productId)) {
            log.error("[InventoryService] Gặp lỗi hệ thống tạm thời khi trừ kho cho sản phẩm {}", productId);
            throw new RuntimeException("Lỗi kết nối cơ sở dữ liệu kho tạm thời (Database timeout)!");
        }

        int currentStock = stockDatabase.getOrDefault(productId, 10);
        if (currentStock < quantity) {
            log.warn("[InventoryService] Tồn kho không đủ! Mã SP: {}, Hiện còn: {}, Yêu cầu: {}", 
                    productId, currentStock, quantity);
            throw new IllegalStateException("Hàng trong kho không đủ để đáp ứng đơn hàng!");
        }

        int updatedStock = currentStock - quantity;
        stockDatabase.put(productId, updatedStock);
        log.info("[InventoryService] Khấu trừ kho thành công! Sản phẩm: {}, Còn lại: {}", productId, updatedStock);
    }

    public int getStock(String productId) {
        return stockDatabase.getOrDefault(productId, 0);
    }
}
