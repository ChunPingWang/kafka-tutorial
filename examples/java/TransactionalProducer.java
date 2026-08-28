import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

/**
 * 交易型 Producer：多筆訊息「全有或全無」地寫入（exactly-once 的寫入端）。
 *
 * 對應 README 8.5 節。重點：
 *   1. transactional.id 必須全域唯一且「重啟後不變」——Kafka 用它做 zombie fencing：
 *      舊行程若還活著，會在新行程 initTransactions() 之後被拒絕寫入。
 *   2. 設定 transactional.id 之後，冪等性（enable.idempotence）自動開啟。
 *   3. 下游 consumer 必須設 isolation.level=read_committed 才看不到被 abort 的訊息。
 *
 * 執行（會先寫入並 commit 3 筆，再寫入並 abort 2 筆）：
 *   ./run-example.sh TransactionalProducer tx-demo
 * 驗證（read_committed 應只看到 3 筆 committed）：
 *   kafka-console-consumer.sh --bootstrap-server $BS --topic tx-demo \
 *     --from-beginning --isolation-level read_committed --timeout-ms 10000
 */
public class TransactionalProducer {
    public static void main(String[] args) throws Exception {
        String topic = args.length > 0 ? args[0] : "tx-demo";
        String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 正式環境請帶上主機或實例識別，例如 "orders-svc-" + instanceId
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-demo-producer-1");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            producer.initTransactions();   // 註冊 transactional.id、fence 掉舊行程

            // --- 交易 1：三筆一起 commit ---
            producer.beginTransaction();
            for (int i = 1; i <= 3; i++) {
                producer.send(new ProducerRecord<>(topic, "order-" + i, "committed-" + i));
            }
            producer.commitTransaction();
            System.out.println("交易 1：3 筆已 commit");

            // --- 交易 2：兩筆之後 abort（模擬中途失敗回滾）---
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "order-4", "aborted-4"));
            producer.send(new ProducerRecord<>(topic, "order-5", "aborted-5"));
            producer.abortTransaction();
            System.out.println("交易 2：2 筆已 abort（read_committed 的 consumer 看不到）");
        }
        System.out.println("完成。用 --isolation-level read_committed 消費 " + topic + " 應只有 3 筆。");
    }
}
