# BÁO CÁO KỸ THUẬT: XỬ LÝ LỖI KAFKA CONSUMER VỚI RETRY VÀ DEAD LETTER QUEUE (DLQ)

---

## PHẦN 1: PHÂN TÍCH CHUYÊN SÂU CƠ CHẾ KAFKA OFFSET VÀ NGUYÊN NHÂN CONSUMER BỊ KẸT

### 1. Cơ chế đọc Offset trong Apache Kafka

#### 1.1. Khái niệm cơ bản về Partition và Offset
- Trong Apache Kafka, mỗi **Topic** được chia thành một hoặc nhiều **Partition**. Mỗi partition là một commit log tuần tự, bất biến (immutable sequence of records).
- Mỗi message trong partition được gán một số nguyên tăng dần duy nhất gọi là **Offset**.
- Có 3 khái niệm offset quan trọng:
  1. **Log End Offset (LEO)**: Vị trí của tin nhắn tiếp theo sẽ được ghi vào partition bởi Producer.
  2. **Current Offset (Position)**: Vị trí của record mà Consumer đang trỏ tới để chuẩn bị đọc trong lần `poll()` tiếp theo.
  3. **Committed Offset**: Vị trí cuối cùng mà Consumer Group đã xác nhận là đã xử lý thành công và lưu vào internal topic `__consumer_offsets` của Kafka.

```
Partition 0: [Msg 0] [Msg 1] [Msg 2] [Msg 3 (Lỗi)] [Msg 4] [Msg 5] ...
                                        ▲                     ▲
                              Committed Offset = 3           LEO = 6
                              Current Position = 3
```

#### 1.2. Cơ chế hoạt động của vòng lặp `poll()` và Commit Offset
- Consumer hoạt động theo mô hình **Pull**: Định kỳ gọi phương thức `consumer.poll(Duration)` để kéo một lô (batch) các records từ broker về bộ nhớ.
- Khi Consumer đọc tin nhắn:
  - **Auto Commit (`enable.auto.commit = true`)**: Mặc định, Consumer sẽ tự động commit offset sau mỗi chu kỳ thời gian (ví dụ `auto.commit.interval.ms = 5000`). Tuy nhiên, việc commit chỉ diễn ra ở đầu mỗi lần gọi `poll()` tiếp theo.
  - **Manual Commit (`enable.auto.commit = false` hoặc Spring `AckMode.RECORD`)**: Ứng dụng chỉ commit offset khi xử lý xong message hoặc đã được recover thành công.

---

### 2. Lý do Consumer bị "Kẹt" (Head-of-Line Blocking / Poison Pill)

Tại `Inventory-Service`, thi thoảng hệ thống nhận được những kiện hàng bị lỗi định dạng JSON (thiếu dấu `}`), hoặc chứa mã sản phẩm không tồn tại (`productId: null`). Tin nhắn này được gọi là **"Poison Pill" (Viên thuốc độc)**.

#### Quá trình gây kẹt diễn ra như sau:
1. **Consumer đọc message**: Consumer kéo tin nhắn tại `Offset N` về.
2. **Xảy ra Exception**:
   - *Trường hợp 1 (Lỗi Deserializer)*: Chuỗi JSON thiếu `}` khiến `JsonDeserializer` quăng `SerializationException` ngay trong luồng `poll()`.
   - *Trường hợp 2 (Lỗi Business Logic)*: Đơn hàng có `productId: null` khiến `InventoryConsumer` quăng `IllegalArgumentException: Mã sản phẩm không tồn tại`.
3. **Offset không được commit**: Do xảy ra Exception, luồng xử lý bị ngắt đột ngột. Offset của message `N` chưa từng được commit. `Committed Offset` trên broker vẫn dừng tại vị trí `N`.
4. **Vòng lặp bất tận (Infinite Loop)**:
   - Consumer lại kéo đúng message lỗi tại `Offset N` về, tiếp tục ném Exception, tiếp tục không commit!
5. **Hệ quả nghiêm trọng (Head-of-Line Blocking)**:
   - Toàn bộ hàng vạn đơn hàng hợp lệ phía sau tại `Offset N+1, N+2, N+3...` trong partition đó bị kẹt cứng, không bao giờ được chạm tới.

---

## PHẦN 2: THIẾT KẾ KIẾN TRÚC VÀ GIẢI PHÁP SỬA CODE

Mô hình phối hợp 3 tầng chịu lỗi (Fault Tolerance):

```
[Incoming Message] 
       │
       ▼
[1. ErrorHandlingDeserializer] ────(Nếu lỗi cú pháp JSON)────┐
       │                                                     │
       ▼ (Nếu JSON hợp lệ)                                  │
[InventoryConsumer.consume()]                                │
       │                                                     │
       ├──(Thành công)──► [Commit Offset & Tiếp tục]         │
       │                                                     │
       ▼ (Nếu ném Exception: productId == null)             │
[2. DefaultErrorHandler (FixedBackOff)] ◄────────────────────┘
       │
       ├──► Thử lại lần 1 (Chờ 2 giây)
       ├──► Thử lại lần 2 (Chờ 2 giây)
       └──► Thử lại lần 3 (Chờ 2 giây)
                 │
                 ▼ (Vẫn thất bại sau 3 lần)
[3. DeadLetterPublishingRecoverer]
       │
       ├──► In log ERROR: "Đã ném đơn hàng bị lỗi vào DLQ" (REQ-02)
       ├──► Gửi record sang topic "storex-order-events.DLQ"
       └──► Commit Offset N trên partition chính ──► Giải phóng Consumer đọc N+1!
```

---

## PHẦN 3: PHÂN TÍCH ĐIỀU KIỆN BIÊN BUG-05 VÀ TIÊU CHÍ REQ-02

### 3.1. BUG-05: Lỗi "The class is not in the trusted packages"
- **Nguyên nhân**: Mặc định, Jackson `JsonDeserializer` của Spring Kafka kiểm tra thông tin Header `__TypeId__` để parse thành Java Class tương ứng. Nhằm ngăn chặn lỗ hổng tấn công thực thi mã độc từ xa (**Remote Code Execution - RCE**), Spring Kafka giới hạn chỉ giải mã các class nằm trong danh sách các package được tin cậy. Nếu không cấu hình, khi nhận object từ service khác, Kafka sẽ từ chối và ném lỗi:
  `IllegalArgumentException: The class '...' is not in the trusted packages`
- **Yêu cầu xử lý bắt buộc**:
  Bổ sung cấu hình `trusted.packages=*` hoặc `spring.json.trusted.packages: "*"` trong file cấu hình và `ConsumerConfig`:
  ```yaml
  spring:
    kafka:
      consumer:
        properties:
          spring.json.trusted.packages: "*"
          trusted.packages: "*"
  ```
  Trong code Java:
  ```java
  props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
  ```

### 3.2. REQ-02: Ghi nhận hành vi ném vào DLQ với Log ERROR
- **Yêu cầu xử lý bắt buộc**:
  Khi một đơn hàng đã được thử lại đủ 3 lần mà vẫn phát sinh ngoại lệ, hệ thống phải in log màu đỏ / mức `ERROR` thông báo chính xác:
  ```
  "Đã ném đơn hàng bị lỗi vào DLQ"
  ```
- **Triển khai trong `DeadLetterPublishingRecoverer`**:
  ```java
  @Bean
  public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, Object> kafkaTemplate) {
      return new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> {
          // REQ-02: In log ERROR bắt buộc
          log.error("==================================================================================");
          log.error("Đã ném đơn hàng bị lỗi vào DLQ");
          log.error("==> Bản tin gốc: [Topic: {}, Partition: {}, Offset: {}, Key: {}]",
                  record.topic(), record.partition(), record.offset(), record.key());
          log.error("==> Nguyên nhân ngoại lệ sau 3 lần retry: {}", ex.getMessage());
          log.error("==> Đích chuyển tiếp: {}", KafkaTopicConfig.STOREX_ORDER_EVENTS_DLQ_TOPIC);
          log.error("==================================================================================");

          return new TopicPartition(KafkaTopicConfig.STOREX_ORDER_EVENTS_DLQ_TOPIC, record.partition());
      });
  }
  ```

---

## PHẦN 4: CHI TIẾT MÃ NGUỒN TRIỂN KHAI

### 4.1. Cấu hình Topic (`KafkaTopicConfig.java`)
```java
@Configuration
public class KafkaTopicConfig {
    public static final String STOREX_ORDER_EVENTS_TOPIC = "storex-order-events";
    public static final String STOREX_ORDER_EVENTS_DLQ_TOPIC = "storex-order-events.DLQ";

    @Bean
    public NewTopic storexOrderEventsTopic() {
        return TopicBuilder.name(STOREX_ORDER_EVENTS_TOPIC).partitions(5).replicas(1).build();
    }

    @Bean
    public NewTopic storexOrderEventsDlqTopic() {
        return TopicBuilder.name(STOREX_ORDER_EVENTS_DLQ_TOPIC).partitions(5).replicas(1).build();
    }
}
```

### 4.2. Cấu hình ErrorHandler & Retry 3 lần x 2s (`KafkaConsumerConfig.java`)
```java
@Bean
public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
    // Retry tối đa 3 lần, mỗi lần cách nhau 2 giây (2000ms)
    FixedBackOff backOff = new FixedBackOff(2000L, 3L);
    DefaultErrorHandler errorHandler = new DefaultErrorHandler(deadLetterPublishingRecoverer, backOff);

    errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
        log.warn("[RETRY-CONTROLLER] Thử lại lần {}/3 cho đơn hàng tại [Topic: {}, Partition: {}, Offset: {}]. Lỗi: {}",
                deliveryAttempt, record.topic(), record.partition(), record.offset(), ex.getMessage());
    });

    return errorHandler;
}
```

### 4.3. Consumer nghiệp vụ Kho (`InventoryConsumer.java`)
```java
@Service
public class InventoryConsumer {
    @KafkaListener(
            topics = KafkaTopicConfig.STOREX_ORDER_EVENTS_TOPIC,
            groupId = "inventory-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(@Payload OrderEvent event, ...) {
        // Kiểm tra điều kiện mã sản phẩm null theo kịch bản đề bài
        if (event.getProductId() == null || event.getProductId().trim().isEmpty()) {
            throw new IllegalArgumentException("Mã sản phẩm không tồn tại (productId: null)!");
        }

        inventoryService.deductStock(event.getProductId(), event.getQuantity());
    }
}
```

### 4.4. Consumer giám sát Dead Letter Queue (`DeadLetterQueueConsumer.java`)
```java
@Service
public class DeadLetterQueueConsumer {
    @KafkaListener(
            topics = KafkaTopicConfig.STOREX_ORDER_EVENTS_DLQ_TOPIC,
            groupId = "inventory-dlq-group"
    )
    public void consumeDeadLetterQueue(@Payload Object payload, ...) {
        log.error("==================== [DEAD LETTER QUEUE (DLQ) TIẾP NHẬN] ====================");
        log.error("Xác nhận: Đã tiếp nhận đơn hàng bị lỗi vào hộp thư chết DLQ!");
        log.error("Nguyên nhân lỗi: {}", exceptionMessage);
        log.error("Payload đơn hàng: {}", payload);
        log.error("=============================================================================");
    }
}
```

---

## PHẦN 5: HƯỚNG DẪN KIỂM THỬ KỊCH BẢN THỰC TẾ

1. **Kiểm thử đơn hàng hợp lệ**:
   - Gọi `POST http://localhost:8084/api/orders/event`
   - Log: Trừ kho thành công, offset được commit ngay lập tức.
2. **Kiểm thử JSON hỏng cú pháp (thiếu `}`)**:
   - Gọi `POST http://localhost:8084/api/orders/malformed`
   - Log: `ErrorHandlingDeserializer` bọc lỗi $\rightarrow$ Retry lần 1 (chờ 2s) $\rightarrow$ Retry lần 2 (chờ 2s) $\rightarrow$ Retry lần 3 (chờ 2s) $\rightarrow$ In log ERROR: `"Đã ném đơn hàng bị lỗi vào DLQ"` $\rightarrow$ Bản tin được chuyển an toàn sang `storex-order-events.DLQ`.
3. **Kiểm thử mã sản phẩm null (`productId: null`)**:
   - Gọi `POST http://localhost:8084/api/orders/null-product`
   - Log: `IllegalArgumentException` $\rightarrow$ Retry 3 lần x 2s $\rightarrow$ In log ERROR: `"Đã ném đơn hàng bị lỗi vào DLQ"` $\rightarrow$ Consumer tiếp tục nhận các đơn kế tiếp mà không bị kẹt partition!
