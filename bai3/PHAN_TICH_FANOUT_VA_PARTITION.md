# BÁO CÁO PHÂN TÍCH KỸ THUẬT: CƠ CHẾ FAN-OUT VÀ THIẾT KẾ PARTITION SCALING TRONG KAFKA

**Học phần:** Kiến trúc Microservices & Hệ thống hướng sự kiện (EDA)  
**Chủ đề:** Khắc phục lỗi cạnh tranh Consumer Group (**BUG-04**) & Thiết kế Partition tối ưu khi Scale-up 3 Instances (**REQ-01**)

---

## PHẦN 1: PHÂN TÍCH LỖI BUG-04 (CẠNH TRANH TRONG CÙNG CONSUMER GROUP)

### 1.1. Hiện trạng lỗi (Bug Scenario)
Trong quá trình phát triển nhanh, lập trình viên đã copy nguyên cấu hình từ `Inventory-Service` sang `Loyalty-Service` và vô tình giữ nguyên thuộc tính:
```yaml
spring:
  kafka:
    consumer:
      group-id: storex-system   # DÙNG CHUNG CHO CẢ KHO VÀ ĐIỂM THƯỞNG
```

### 1.2. Hậu quả thực tế trên môi trường nghiệp vụ
- Khách hàng thanh toán đơn hàng thành công, kho hàng trừ tồn kho chính xác nhưng tài khoản khách **hoàn toàn không được cộng điểm thưởng**.
- Ngược lại, có những đơn hàng khách được cộng điểm thưởng nhưng kho lại **không nhận được thông tin để đóng gói và trừ tồn kho**, dẫn tới trễ hạn giao hàng.
- Tỷ lệ mất mát: Xấp xỉ $50\%$ số đơn không được trừ kho và $50\%$ số đơn không được cộng điểm!

### 1.3. Bản chất nguyên nhân kiến trúc (Root Cause Analysis)
Apache Kafka quản lý việc phân phối dữ liệu dựa trên **Consumer Group**:

```
                              [Topic: storex-order-events]
                                /                      \
                    Partition 0                         Partition 1
                         │                                   │
                         ▼                                   ▼
             [Consumer 1: Kho Hàng]              [Consumer 2: Điểm Thưởng]
                   (Nhận Msg 1, 3, 5)                  (Nhận Msg 2, 4, 6)
             
             ==> CẢ HAI ĐANG NẰM CHUNG TRONG GROUP "storex-system"
             ==> MÔ HÌNH: POINT-TO-POINT (TRANH CHẤP / CHIA TẢI)
```

1. **Mô hình Point-to-Point (Queue) trong cùng 1 Consumer Group**:
   - Khi nhiều Consumer cùng khai báo một `group-id`, Kafka Broker hiểu rằng đây là các thực thể (instances) của **cùng một dịch vụ** đang muốn **chia tải (load balance)**.
   - Mỗi Partition trong Topic chỉ được gán cho **duy nhất một Consumer** trong cùng Group tại một thời điểm.
   - Do đó, nếu Partition 0 được gán cho `Inventory-Service`, thì các tin nhắn trong Partition 0 chỉ gửi tới `Inventory-Service`. `Loyalty-Service` hoàn toàn không bao giờ nhìn thấy các tin nhắn này!

2. **Hệ quả của sự tranh chấp**:
   - Thay vì hoạt động theo cơ chế **Publish-Subscribe (Fan-out)** để cả hai dịch vụ cùng nhận $100\%$ đơn hàng, chúng lại rơi vào trạng thái **cạnh tranh (competing consumers)**. Đơn nào rơi vào Kho thì Điểm thưởng bị bỏ lỡ, và ngược lại.

---

### 1.4. Triển khai cấu hình chuẩn xác (The Fix)
Để đạt được cơ chế **Fan-out (Broadcast $100\%$ message)**, mỗi microservice độc lập bắt buộc phải có một **Group ID riêng biệt**:

```
                                  [Topic: storex-order-events]
                                     /                     \
                      (Fan-out 100%)                         (Fan-out 100%)
                           /                                     \
               Group: "inventory-group"                  Group: "loyalty-group"
              ┌────────────────────────┐                ┌───────────────────────┐
              │   Inventory-Service    │                │    Loyalty-Service    │
              │  (Trừ tồn kho 100%)    │                │ (Cộng điểm thưởng 100%)│
              └────────────────────────┘                └───────────────────────┘
```

#### File cấu hình `application.yml` chuẩn mực:
```yaml
server:
  port: 8083

spring:
  application:
    name: storex-consumer-service
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      auto-offset-reset: earliest
      enable-auto-commit: true

# =========================================================
# KHẮC PHỤC BUG-04: ĐẶT GROUP-ID ĐỘC LẬP CHO TỪNG SERVICE
# =========================================================

# 1. Dịch vụ Kho (Inventory)
inventory:
  kafka:
    consumer:
      group-id: inventory-group   # Nhóm độc lập cho Kho
      concurrency: 3              # Scale 3 instances/threads

# 2. Dịch vụ Điểm thưởng (Loyalty)
loyalty:
  kafka:
    consumer:
      group-id: loyalty-group     # Nhóm độc lập cho Loyalty
      concurrency: 1
```

---

## PHẦN 2: THIẾT KẾ PARTITION TỐI ƯU CHO INVENTORY-SERVICE (REQ-01)

### 2.1. Đề xuất số lượng Partition tối thiểu
**Đề xuất:** Số lượng Partition tối thiểu của Topic `storex-order-events` phải là **3 Partitions**.

### 2.2. Cơ sở lý thuyết và Quy tắc phân bổ Kafka Partition
Trong Kafka, nguyên tắc gán Partition cho Consumer trong cùng 1 Consumer Group như sau:
$$\text{Số Consumer Instance hoạt động thực tế} \le \text{Số lượng Partition}$$

1. **Trường hợp $\text{Partitions} < \text{Số Consumer Instances}$ (Ví dụ: 2 Partitions, 3 Instances)**:
   - Partition 0 $\rightarrow$ Instance 1
   - Partition 1 $\rightarrow$ Instance 2
   - **Instance 3 $\rightarrow$ Rơi vào trạng thái "chết đói" (Idle/Starvation)**, hoàn toàn không được gán partition nào và không xử lý bất kỳ byte dữ liệu nào, gây lãng phí $33\%$ chi phí hạ tầng máy chủ!
2. **Trường hợp $\text{Partitions} = \text{Số Consumer Instances}$ (Ví dụ: 3 Partitions, 3 Instances)**:
   - Mỗi instance đảm nhận xử lý đúng 1 partition.
   - Tỷ lệ chia tải đạt mức lý tưởng: Mỗi server xử lý chính xác $33.33\%$ lượng đơn hàng.
3. **Trường hợp $\text{Partitions} > \text{Số Consumer Instances}$ (Ví dụ: 5 Partitions đã tạo ở Bài 2, 3 Instances)**:
   - Kafka Partitioner tự động phân bổ:
     - Instance 1: Phụ trách Partition 0, Partition 1 (Gánh $40\%$ tải)
     - Instance 2: Phụ trách Partition 2, Partition 3 (Gánh $40\%$ tải)
     - Instance 3: Phụ trách Partition 4 (Gánh $20\%$ tải)
   - Tất cả 3 instance đều tham gia xử lý tích cực, không có instance nào bị idle. Hơn nữa, kiến trúc này sẵn sàng để scale-out lên tới 5 instances trong các dịp Mega Sale mà không cần repartitioning!

---

### 2.3. Bảng so sánh các phương án thiết kế Partition

| Tiêu chí | 1 Partition | 3 Partitions | 5 Partitions (Đã chọn) | 10 Partitions |
| :--- | :--- | :--- | :--- | :--- |
| **Khả năng chạy 3 instance Inventory** | Thất bại (2 instance idle) | Hoàn hảo (Mỗi máy 1 partition) | Hoàn hảo (Chia tải 2-2-1) | Tốt (Chia tải 4-3-3) |
| **Xử lý Mega Sale 10,000 đơn/giây** | Nghẽn cổ chai (Lag cực lớn) | Đạt ngưỡng (~3,333 đơn/máy) | Rất tốt (~2,000 đơn/partition) | Cực tốt (~1,000 đơn/partition) |
| **Khả năng Scale-up mở rộng trong tương lai** | Không thể | Giới hạn tối đa 3 máy | Có thể nâng lên tối đa 5 máy | Nâng lên tối đa 10 máy |
| **Overhead duy trì kết nối & ZooKeeper/KRaft** | Rất thấp | Thấp | Tối ưu | Trung bình |

**Kết luận REQ-01:**
- Số lượng Partition tối thiểu để 3 instance Inventory-Service hoạt động hiệu quả là **3 Partitions**.
- Việc Topic `storex-order-events` được khởi tạo với **5 Partitions** từ Bài 2 là hoàn toàn tối ưu, vừa đáp ứng trọn vẹn yêu cầu chia tải 3 instances, vừa có sẵn dư địa để scale lên 5 instances khi lưu lượng đột biến.

---

## PHẦN 3: KẾT QUẢ KIỂM THỬ THỰC TẾ

1. **Kiểm thử Fan-out qua API `/api/orders/simulate-batch/30`**:
   - Gửi đồng loạt 30 đơn hàng vào Kafka.
   - **Kết quả tại Inventory:** Xử lý đủ 30/30 đơn, log ghi nhận 3 threads (`inventoryKafkaListenerContainerFactory-0, 1, 2`) thay phiên nhau xử lý các partition.
   - **Kết quả tại Loyalty:** Xử lý đủ 30/30 đơn, tích điểm thành công cho toàn bộ khách hàng mà không bị sót bất kỳ đơn nào.
2. **Kiểm tra API `/api/orders/stats`**:
   ```json
   {
     "inventoryProcessedCount": 30,
     "loyaltyProcessedCount": 30,
     "note": "Nếu hai số bằng nhau, cơ chế Fan-out 100% đã hoạt động thành công!"
   }
   ```
   $\rightarrow$ Xác nhận cơ chế Fan-out và Partition chia tải đã được giải quyết hoàn toàn.
