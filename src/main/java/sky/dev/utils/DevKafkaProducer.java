package sky.dev.utils;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.intellij.lang.annotations.Language;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class DevKafkaProducer {

    public static final String TOPIC = "input-topic";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";

    public static void main(String[] args) {
        try (
                KafkaProducer<String, String> producer = new KafkaProducer<>(
                        Map.of(
                                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                BOOTSTRAP_SERVERS,
                                ProducerConfig.CLIENT_ID_CONFIG,
                                UUID.randomUUID().toString()
                        ),
                        new StringSerializer(),
                        new StringSerializer()
                );
        ) {

            @Language("JSON") String a1 = """
                    { "name": "Jorge", "age": 30, "job": "Developer"}
                    """;
            @Language("JSON") String b1 = """
                    { "name": "Bob", "age": 32, "job": "Developer"}
                    """;
            @Language("JSON") String c1 = """
                    { "name": "Ou", "age": 42, "job": "Ba"}
                    """;
            @Language("JSON") String d1 = """
                    { "name": "Kim", "age": 54, "job": "Daaa"}
                    """;
            @Language("JSON") String e1 = """
                    { "name": "Bob", "age": 252423, "job": "Bla"}
                    """;

            producer.send(new ProducerRecord<>(TOPIC, "testcontainers", a1)).get();
            producer.send(new ProducerRecord<>(TOPIC, "testcontainers", b1)).get();
            producer.send(new ProducerRecord<>(TOPIC, "testcontainers", c1)).get();
            producer.send(new ProducerRecord<>(TOPIC, "testcontainers", d1)).get();
            producer.send(new ProducerRecord<>(TOPIC, "testcontainers", e1)).get();

        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
