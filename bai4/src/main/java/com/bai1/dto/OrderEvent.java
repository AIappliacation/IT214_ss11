package com.bai1.dto;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

public class OrderEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String orderId;
    private String productId;
    private int quantity;
    private BigDecimal price;
    private String customerId;
    private LocalDateTime orderDate;
    private String status;

    public OrderEvent() {
    }

    public OrderEvent(String orderId, String productId, int quantity, BigDecimal price, String customerId) {
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.price = price;
        this.customerId = customerId;
        this.orderDate = LocalDateTime.now();
        this.status = "NEW";
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public LocalDateTime getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(LocalDateTime orderDate) {
        this.orderDate = orderDate;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", productId='" + productId + '\'' +
                ", quantity=" + quantity +
                ", price=" + price +
                ", customerId='" + customerId + '\'' +
                ", orderDate=" + orderDate +
                ", status='" + status + '\'' +
                '}';
    }
}
