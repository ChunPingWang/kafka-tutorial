import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

/**
 * Kafka Streams 入門：word count（串流處理的 "Hello World"）。
 *
 * Kafka Streams 是「函式庫」不是「叢集」——它就在你的應用程式行程裡跑，
 * 用 consumer group 做水平擴充（多開幾個行程就自動分攤 partition），
 * 中間狀態存在本機 RocksDB + Kafka changelog topic（行程掛掉可重建）。
 *
 * 拓撲：
 *   words-input（文字行）
 *     → flatMap 拆單字 → groupBy 單字 → count()（有狀態！）
 *     → words-output（單字, 累計次數）
 *
 * 執行：
 *   kafka-topics.sh --bootstrap-server $BS --create --topic words-input --partitions 3 --replication-factor 3
 *   ./run-example.sh WordCountStream
 * 灌資料與看結果：
 *   printf 'hello kafka\nhello streams\n' | kafka-console-producer.sh --bootstrap-server $BS --topic words-input
 *   kafka-console-consumer.sh --bootstrap-server $BS --topic words-output --from-beginning \
 *     --property print.key=true --timeout-ms 15000
 */
public class WordCountStream {
    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
        long runForMs    = Long.parseLong(System.getenv().getOrDefault("RUN_FOR_MS", "30000"));

        Properties props = new Properties();
        // application.id 同時是 consumer group id 與內部 topic 的前綴
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // 範例縮短 commit 間隔讓結果快點可見；正式環境用預設值（30s, EOS 下另計）
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "2000");
        props.put(StreamsConfig.STATE_DIR_CONFIG,
                System.getenv().getOrDefault("STREAMS_STATE_DIR", "/tmp/kafka-streams-wordcount"));

        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream("words-input")
               .flatMapValues(line -> Arrays.asList(line.toLowerCase(Locale.ROOT).split("\\W+")))
               .filter((k, word) -> !word.isBlank())
               .groupBy((k, word) -> word)          // 依單字重新分區（會建立 repartition topic）
               .count()                             // 有狀態運算（RocksDB + changelog topic）
               .toStream()
               .mapValues(Object::toString)
               .to("words-output", Produced.with(Serdes.String(), Serdes.String()));

        try (KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
            streams.start();
            System.out.println("WordCount 運行中（" + runForMs + " ms 後自動結束）...");
            Thread.sleep(runForMs);
        }
        System.out.println("已結束。累計結果在 words-output。");
    }
}
