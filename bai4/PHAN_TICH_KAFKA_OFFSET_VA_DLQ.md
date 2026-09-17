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
  - **Manual Commit (`enable.auto.commit = false` hoặc Spring `AckMode`)**: Ứng dụng chỉ commit offset khi xử lý xong message hoặc lô message thành công.

---

### 2. Lý do Consumer bị "Kẹt" (Head-of-Line Blocking / Poison Pill)

Khi hệ thống đặt hàng vô tình sinh ra chuỗi JSON sai định dạng (ví dụ thiếu dấu `}`), tin nhắn này được gọi là **"Poison Pill" (Viên thuốc độc)**.

#### Quá trình gây kẹt diễn ra như sau:
1. **Consumer đọc message**: Consumer kéo tin nhắn tại `Offset N` về.
2. **Xảy ra Exception**:
   - *Trường hợp 1 (Lỗi Deserializer)*: Chuỗi JSON thiếu `}` khiến `JsonDeserializer` quăng `SerializationException` ngay trong luồng `poll()`.
   - *Trường hợp 2 (Lỗi Business Logic)*: Dữ liệu bị null hoặc nghiệp vụ trừ kho quăng `RuntimeException`.
3. **Offset không được commit**: Do xảy ra Exception, code xử lý bị ngắt đột ngột. Hàm commit offset của message `N` chưa từng được thực thi. `Committed Offset` trên broker của Consumer Group vẫn nằm tại vị trí `N`.
4. **Vòng lặp bất tận**:
   - Vòng lặp listener phát hiện lỗi và dừng xử lý lô hiện tại.
   - Khi Consumer tiếp tục gọi `poll()` hoặc xảy ra tái cân bằng (rebalance) / khởi động lại, Consumer sẽ hỏi Kafka Broker: *"Tôi cần đọc từ đâu?"*. Broker trả lời: *"Đọc từ Committed Offset = N"*.
   - Consumer lại kéo đúng message lỗi tại `Offset N` về, tiếp tục ném Exception, tiếp tục không commit!
5. **Hệ quả nghiêm trọng**:
   - Toàn bộ các message hợp lệ xếp hàng phía sau tại `Offset N+1, N+2, N+3...` trong partition đó hoàn toàn không bao giờ được chạm tới (**Head-of-Line Blocking**).
   - Kho hàng StoreX bị đình trệ toàn diện.

---

## PHẦN 2: THIẾT KẾ KIẾN TRÚC VÀ GIẢI PHÁP SỬA CODE

Để giải quyết triệt để vấn đề trên mà không làm mất mát dữ liệu và không làm tắc nghẽn hệ thống, chúng ta áp dụng mô hình phối hợp 3 tầng:

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
       ▼ (Nếu ném Exception)                                │
[2. DefaultErrorHandler (FixedBackOff)] ◄────────────────────┘
       │
       ├──► Thử lại lần 1 (Chờ 1000ms)
       ├──► Thử lại lần 2 (Chờ 1000ms)
       └──► Thử lại lần 3 (Chờ 1000ms)
                 │
                 ▼ (Vẫn thất bại sau 3 lần)
[3. DeadLetterPublishingRecoverer]
       │
       ├──► Gửi record sang topic "order-events.DLT"
       └──► Commit Offset N trên topic chính ──► Giải phóng Consumer đọc N+1!
```

### 1. Tầng 1: `ErrorHandlingDeserializer`
- Nếu dùng `JsonDeserializer` thông thường, khi JSON thiếu `}` nó sẽ throw exception làm chết `poll()` trước khi listener container kịp can thiệp.
- `ErrorHandlingDeserializer` bọc ngoài `JsonDeserializer`. Khi gặp JSON hỏng, nó không quăng ngoại lệ ra ngoài luồng mà bọc lại thành `DeserializationException` trong record header và chuyển tiếp sang cho Spring Listener Error Handler xử lý.

### 2. Tầng 2: `DefaultErrorHandler` kết hợp `FixedBackOff`
- Cung cấp cơ chế thử lại (Retry) với `FixedBackOff(1000L, 3L)`: Thử lại tối đa 3 lần, mỗi lần cách nhau 1000ms.
- Giúp tự phục hồi đối với các lỗi tạm thời (transient errors) như chập chờn mạng, database lock timeout, microservice khác tạm thời không phản hồi.

### 3. Tầng 3: `DeadLetterPublishingRecoverer` (DLQ)
- Khi đã vượt quá số lần retry cho phép (3 lần) mà vẫn thất bại:
  - `DeadLetterPublishingRecoverer` sẽ tự động publish toàn bộ message lỗi sang topic Dead Letter Queue: `order-events.DLT`.
  - Đồng thời gắn kèm các header chẩn đoán quan trọng:
    - `dlt-original-topic`: Topic gốc (`order-events`).
    - `dlt-original-partition`: Partition xảy ra lỗi.
    - `dlt-original-offset`: Offset của message bị lỗi.
    - `dlt-exception-message`: Thông điệp lỗi chi tiết.
    - `dlt-exception-stacktrace`: Toàn bộ stack trace ngoại lệ.
  - Sau khi chuyển vào DLQ thành công, ErrorHandler tiến hành **commit offset** của message lỗi trên topic gốc.
  - Nhờ vậy, Consumer được giải phóng và lập tức chuyển sang xử lý các message tiếp theo `N+1`!

---

## PHẦN 3: CHI TIẾT MÃ NGUỒN TRIỂN KHAI

### 1. Cấu hình ErrorHandler & Deserializer (`KafkaConsumerConfig.java`)
```java
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:inventory-group}")
    private String groupId;

    // 1. Cấu hình ConsumerFactory với ErrorHandlingDeserializer
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.bai1.dto.OrderEvent");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // 2. Cấu hình DeadLetterPublishingRecoverer đẩy message sang <topic>.DLT
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, Object> kafkaTemplate) {
        return new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> 
            new TopicPartition(record.topic() + ".DLT", record.partition())
        );
    }

    // 3. Cấu hình DefaultErrorHandler: Retry tối đa 3 lần, mỗi lần cách nhau 1000ms
    @Bean
    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
        FixedBackOff backOff = new FixedBackOff(1000L, 3L);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.warn("[RETRY-HANDLER] Thử lại lần thứ {}/3 cho message tại [Topic: {}, Offset: {}]. Lỗi: {}",
                    deliveryAttempt, record.topic(), record.offset(), ex.getMessage());
        });

        return errorHandler;
    }

    // 4. ContainerFactory với AckMode RECORD
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler errorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }
}
```

### 2. Consumer nghiệp vụ (`InventoryConsumer.java`)
```java
@Service
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);
    private final InventoryService inventoryService;

    public InventoryConsumer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaListener(
            topics = "order-events",
            groupId = "inventory-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("==> [InventoryConsumer] Nhận tin nhắn [Topic: {}, Partition: {}, Offset: {}]", 
                topic, partition, offset);
        
        // Khấu trừ tồn kho
        inventoryService.deductStock(event.getProductId(), event.getQuantity());

        log.info("<== [InventoryConsumer] Đã trừ kho thành công cho OrderId: {}", event.getOrderId());
    }
}
```

### 3. Consumer giám sát Dead Letter Queue (`DeadLetterQueueConsumer.java`)
```java
@Service
public class DeadLetterQueueConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueConsumer.class);

    @KafkaListener(topics = "order-events.DLT", groupId = "inventory-dlq-group")
    public void consumeDeadLetterQueue(
            @Payload Object payload,
            @Header(value = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) String originalTopic,
            @Header(value = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false) Long originalOffset,
            @Header(value = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage
    ) {
        log.error("==================== [DEAD LETTER QUEUE (DLQ) ALERT] ====================");
        log.error("Tin nhắn bị lỗi sau 3 lần retry! Gốc: [Topic: {}, Offset: {}]", originalTopic, originalOffset);
        log.error("Nguyên nhân lỗi: {}", exceptionMessage);
        log.error("Nội dung payload lỗi: {}", payload);
        log.error("=========================================================================");
    }
}
```

### 4. Cấu hình `application.yml`
```yaml
server:
  port: 8084

spring:
  application:
    name: inventory-service

  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: inventory-group
      auto-offset-reset: earliest
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.key.delegate.class: org.apache.kafka.common.serialization.StringDeserializer
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
        spring.json.trusted.packages: "*"
        spring.json.value.default.type: com.bai1.dto.OrderEvent
        spring.json.use.type.headers: false
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer

logging:
  level:
    root: INFO
    com.bai1: DEBUG
    org.springframework.kafka: INFO
    org.apache.kafka: WARN
```

---

## PHẦN 4: QUY TRÌNH XỬ LÝ DEAD LETTER QUEUE TRONG DOANH NGHIỆP

Trong môi trường Production thực tế, các message trong DLQ không được bỏ mặc mà được vận hành theo quy trình chuyên nghiệp:
1. **Cảnh báo tức thời (Real-time Alerting)**:
   - Hệ thống giám sát (Prometheus, Grafana, Datadog) theo dõi số lượng tin nhắn trong topic `*.DLT`.
   - Nếu có tin nhắn mới trong DLQ, lập tức gửi alert qua Slack/Telegram hoặc PagerDuty cho đội On-call.
2. **Lưu trữ kiểm toán (Audit Storage)**:
   - Consumer DLQ đọc tin nhắn và lưu vào cơ sở dữ liệu (ví dụ bảng `failed_messages` gồm: `payload, reason, original_topic, original_offset, created_at, status='PENDING'`).
3. **Phân loại lỗi**:
   - *Lỗi cú pháp (Poison Pill)*: Do hệ thống Producer bị bug tạo sai JSON -> Báo cho team Producer sửa lỗi; viết script fix dữ liệu trong DB.
   - *Lỗi dữ liệu nghiệp vụ*: Ví dụ mã sản phẩm không tồn tại -> Chuyển sang bộ phận nghiệp vụ/CSKH kiểm tra đơn hàng.
4. **Cơ chế Replay (Thử lại thủ công hoặc tự động)**:
   - Cung cấp trang Admin Dashboard hoặc REST API (`POST /api/admin/dlq/replay/{id}`) để sau khi sửa dữ liệu hợp lệ, quản trị viên có thể đẩy lại message vào topic chính `order-events` để kho hàng tự động xử lý lại đơn hàng mà không bị thất thoát doanh thu.
