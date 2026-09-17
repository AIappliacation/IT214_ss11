# BÁO CÁO KỸ THUẬT BÀI 2: XÂY DỰNG API ĐẶT HÀNG BẤT ĐỒNG BỘ (EVENT PRODUCER) VỚI SPRING WEBFLUX

---

## 1. Mục tiêu và Kiến trúc hệ thống
Trong hệ thống thương mại điện tử StoreX, quy trình đặt hàng liên quan tới nhiều tác vụ nặng: kiểm tra tồn kho, khóa sản phẩm, trừ tiền thẻ tín dụng, tính điểm tích lũy, gửi email thông báo.
Nếu xử lý đồng bộ (Synchronous HTTP Request-Response), người dùng sẽ phải chờ 3 - 5 giây và hệ thống dễ dàng sụp đổ khi tải cao (Mega Sale).

**Giải pháp kiến trúc:**
- Áp dụng mô hình **Event-Driven Architecture (EDA)** kết hợp **Spring WebFlux (Reactive Streams / Non-blocking I/O)**.
- Khi người dùng gửi request `POST /api/v1/orders`, Order-Service đóng gói sự kiện `order.created`, đẩy vào Kafka topic `storex-order-events` và phản hồi ngay lập tức **HTTP Status 202 Accepted** kèm `orderId` dưới dạng `Mono<String>`.

```
[Khách hàng] ──(POST /api/v1/orders)──► [OrderController (WebFlux)]
     ▲                                            │
     │ (202 Accepted: {orderId})                 ▼ (Asynchronous Produce)
     └────────────────────────────────── [Kafka Topic: storex-order-events (5 Partitions)]
                                                  │ Key: orderId
                                                  ▼
                                      [Partition X cố định theo orderId]
```

---

## 2. Phân tích chuyên sâu BUG-03: Lỗi mất trật tự sự kiện khi không chỉ định Key

### 2.1. Hiện trạng lỗi BUG-03
Topic `storex-order-events` được cấu hình có **5 Partitions**.
Nếu Producer gửi message bằng lệnh:
```java
// LỖI: Không truyền Key (Key = null)
kafkaTemplate.send("storex-order-events", orderEvent);
```
Trong Apache Kafka, khi `key == null`:
- Các bản tin sẽ được phân bổ theo thuật toán **Sticky Partitioner** hoặc **Round-Robin** lần lượt vào Partition 0, 1, 2, 3, 4.

### 2.2. Hậu quả nghiệp vụ
Một đơn hàng có vòng đời gồm nhiều sự kiện liên tiếp:
1. `order.created` (Tạo đơn)
2. `order.paid` (Đã thanh toán)
3. `order.cancelled` (Hủy đơn do người dùng đổi ý)

Nếu không có Key:
- `order.created` rơi vào **Partition 0**
- `order.cancelled` rơi vào **Partition 3**

Do các Consumer đọc các Partition độc lập và song song với tốc độ khác nhau:
- Consumer đọc Partition 3 xử lý nhanh hơn, thực hiện hủy đơn trước.
- Consumer đọc Partition 0 xử lý chậm hơn, lát sau mới thực hiện trừ kho và kích hoạt giao hàng.
=> **Kết quả:** Đơn hàng đã hủy nhưng kho vẫn xuất hàng, gây thất thoát nghiêm trọng!

### 2.3. Giải pháp khắc phục bắt buộc
Bắt buộc phải truyền `orderId` làm khóa phân tuyến (**Key**) khi gọi lệnh `send()`:
```java
kafkaTemplate.send("storex-order-events", orderId, orderEvent);
```

**Nguyên lý hoạt động của Kafka Default Partitioner:**
$$\text{Partition} = \text{murmur2}(\text{serializedKey}) \pmod{\text{numPartitions}}$$
- Vì cùng một `orderId` luôn cho ra một giá trị hash cố định, tất cả các sự kiện (`order.created`, `order.paid`, `order.cancelled`) thuộc về cùng một đơn hàng chắc chắn $100\%$ được ghi vào **cùng một Partition duy nhất**.
- Kafka cam kết **Strict Ordering per Partition**, do đó Consumer sẽ luôn đọc và xử lý các sự kiện theo đúng thứ tự thời gian phát sinh của đơn hàng đó.

---

## 3. Chi tiết mã nguồn triển khai

### 3.1. Cấu hình Topic 5 Partitions (`KafkaTopicConfig.java`)
```java
@Configuration
public class KafkaTopicConfig {
    public static final String STOREX_ORDER_EVENTS_TOPIC = "storex-order-events";

    @Bean
    public NewTopic storexOrderEventsTopic() {
        return TopicBuilder.name(STOREX_ORDER_EVENTS_TOPIC)
                .partitions(5) // 5 partitions theo yêu cầu
                .replicas(1)
                .build();
    }
}
```

### 3.2. Cấu hình Serializer (`KafkaProducerConfig.java`)
```java
@Configuration
public class KafkaProducerConfig {
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
```

### 3.3. Endpoint Reactive WebFlux (`OrderController.java`)
```java
@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<String> createOrder(@RequestBody(required = false) OrderRequest request) {
        String orderId = (request != null && request.getOrderId() != null)
                ? request.getOrderId()
                : "ORD-" + UUID.randomUUID().toString().substring(0, 8);

        OrderEvent orderEvent = new OrderEvent(
                orderId,
                request.getCustomerId(),
                request.getProductId(),
                request.getQuantity(),
                request.getTotalPrice(),
                request.getDeliveryAddress()
        );

        // Khóa định tuyến là orderId -> Khắc phục BUG-03
        kafkaTemplate.send(KafkaTopicConfig.STOREX_ORDER_EVENTS_TOPIC, orderId, orderEvent);

        // Trả về ngay lập tức HTTP 202 Accepted kèm orderId (Non-blocking)
        return Mono.just(orderId);
    }
}
```

---

## 4. Hướng dẫn kiểm thử

### 4.1. Gửi request đặt hàng (cURL)
```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-8888",
    "productId": "PROD-IPHONE16",
    "quantity": 2,
    "totalPrice": 2500.00,
    "deliveryAddress": "Hà Nội, Việt Nam"
  }'
```

### 4.2. Kết quả nhận được
- **HTTP Status:** `202 Accepted`
- **Body:** `"ORD-a1b2c3d4"` (Thời gian phản hồi tức thì < 15ms)
- **Kafka Log:** Message được ghi vào Topic `storex-order-events` với Key `"ORD-a1b2c3d4"`. Mọi bản tin kế tiếp có cùng Key này luôn đi vào cùng Partition.
