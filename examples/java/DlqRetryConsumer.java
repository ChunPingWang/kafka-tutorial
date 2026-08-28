import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * 重試 topic + 死信佇列（DLQ）消費者——Kafka 沒有內建重試，這是業界標準做法。
 *
 * 模式（對應 README 第 25 章）：
 *
 *   orders ──處理失敗──> orders.retry ──重試仍失敗（>MAX_RETRIES）──> orders.dlq
 *      ↑                      │
 *      └──── 同一個 consumer 也訂閱 retry topic，帶著 x-retry-count header 重新處理
 *
 * 三個關鍵決定：
 *   1. 手動 commit（enable.auto.commit=false）：訊息「處理完或已送進 retry/DLQ」
 *      之後才 commit，行程掛掉頂多重複處理，不會弄丟。
 *   2. 失敗的訊息「送走再前進」：絕不能卡在原地無限重試，那會堵死整個 partition
 *      （Kafka 是循序消費，一筆卡住，後面全部動不了）。
 *   3. DLQ 訊息帶齊 forensics headers（來源 topic/partition/offset、錯誤訊息、時間），
 *      事後才有辦法human review 或批次重放。
 *
 * 執行（處理 orders 與 orders.retry；value 含 "boom" 的訊息會模擬處理失敗）：
 *   ./run-example.sh DlqRetryConsumer orders
 * 灌測試資料：
 *   printf 'ok-1\nboom-2\nok-3\n' | kafka-console-producer.sh --bootstrap-server $BS --topic orders
 */
public class DlqRetryConsumer {
    static final int MAX_RETRIES = 3;

    public static void main(String[] args) throws Exception {
        String mainTopic  = args.length > 0 ? args[0] : "orders";
        String retryTopic = mainTopic + ".retry";
        String dlqTopic   = mainTopic + ".dlq";
        String bootstrap  = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
        long runForMs     = Long.parseLong(System.getenv().getOrDefault("RUN_FOR_MS", "30000"));

        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, mainTopic + "-processor");
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");           // 關鍵 1
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cp.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pp.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");            // acks=all 隨之生效

        long deadline = System.currentTimeMillis() + runForMs;
        try (Consumer<String, String> consumer = new KafkaConsumer<>(cp);
             Producer<String, String> producer = new KafkaProducer<>(pp)) {
            consumer.subscribe(List.of(mainTopic, retryTopic));
            System.out.printf("訂閱 %s + %s，DLQ=%s，最多重試 %d 次；執行 %d ms%n",
                    mainTopic, retryTopic, dlqTopic, MAX_RETRIES, runForMs);

            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> r : records) {
                    int attempt = retryCount(r) ;
                    try {
                        process(r);                                          // 你的業務邏輯
                        System.out.printf("  ✔ 處理成功 %s[%d]@%d value=%s%n",
                                r.topic(), r.partition(), r.offset(), r.value());
                    } catch (Exception e) {
                        if (attempt < MAX_RETRIES) {                         // 關鍵 2：送走再前進
                            send(producer, retryTopic, r, attempt + 1, e);
                            System.out.printf("  ↻ 第 %d 次失敗，送往 %s：%s%n",
                                    attempt + 1, retryTopic, r.value());
                        } else {
                            send(producer, dlqTopic, r, attempt, e);         // 關鍵 3
                            System.out.printf("  ✘ 重試 %d 次仍失敗，送進 DLQ：%s%n",
                                    attempt, r.value());
                        }
                    }
                }
                if (!records.isEmpty()) consumer.commitSync();               // 處理完才 commit
            }
        }
    }

    /** 模擬業務處理：value 含 "boom" 就丟例外 */
    static void process(ConsumerRecord<String, String> r) {
        if (r.value() != null && r.value().contains("boom"))
            throw new IllegalStateException("模擬的處理失敗：" + r.value());
    }

    static int retryCount(ConsumerRecord<String, String> r) {
        Header h = r.headers().lastHeader("x-retry-count");
        return h == null ? 0 : Integer.parseInt(new String(h.value(), StandardCharsets.UTF_8));
    }

    static void send(Producer<String, String> p, String topic,
                     ConsumerRecord<String, String> src, int retryCount, Exception err) {
        ProducerRecord<String, String> out = new ProducerRecord<>(topic, src.key(), src.value());
        out.headers()
           .add("x-retry-count",  String.valueOf(retryCount).getBytes(StandardCharsets.UTF_8))
           .add("x-orig-topic",   src.topic().getBytes(StandardCharsets.UTF_8))
           .add("x-orig-offset",  (src.partition() + "@" + src.offset()).getBytes(StandardCharsets.UTF_8))
           .add("x-error",        String.valueOf(err.getMessage()).getBytes(StandardCharsets.UTF_8))
           .add("x-failed-at",    String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
        p.send(out);
        p.flush();   // 範例求簡單；高吞吐時改成批次 flush 或倚賴 delivery.timeout.ms
    }
}
