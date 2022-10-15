package sky.dev.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import sky.model.TargetFile;
import sky.model.TargetSon;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DevSonKafkaProducer {
    public static final short REPLICATION_FACTOR = 1;
    public static final int NUM_PARTITIONS = 1;
    public static final String TOPIC = "input";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS))) {
            Collection<NewTopic> topics = Collections.singletonList(new NewTopic(TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR));
            if (!adminClient.listTopics().names().get().contains(TOPIC)) {
                adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
            }
        }
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
            String id1 = "ID1";
            String id2 = "ID2";
            var son1 = TargetSon.builder()
                    .id(id1)
                    .itemId(id1)
                    .coolId("47189473982148932194")
                    .hopId("Ou")
                    .createdDate(new Date())
                    .files(List.of(
                            TargetFile.builder()
                                    .parentId(id1)
                                    .rootId(id1)
                                    .fileId("SomeFile")
                                    .itemType("Image")
                                    .text("SomeTEXT")
                                    .html("<some>html</some>")
                                    .build(),
                            TargetFile.builder()
                                    .parentId(id1)
                                    .rootId(id1)
                                    .fileId("SomeFile2")
                                    .itemType("Zip")
                                    .text("SomeTEXT2")
                                    .html("<some>html2</some>")
                                    .build(),
                            TargetFile.builder()
                                    .parentId(id1)
                                    .rootId(id1)
                                    .fileId("SomeFile3")
                                    .itemType("Zip")
                                    .text("SomeTEXT3")
                                    .html("<some>html3</some>")
                                    .build()
                    ))
                    .build();
            var son2 = TargetSon.builder()
                    .id(id1)
                    .itemId(id1)
                    .coolId("47189473982148932194")
                    .hopId("Bob")
                    .createdDate(new Date())
                    .files(List.of(
                            TargetFile.builder()
                                    .parentId(id1)
                                    .rootId(id1)
                                    .fileId("SomeFile")
                                    .text("SomeTEXT")
                                    .html("<some>html</some>")
                                    .build(),
                            TargetFile.builder()
                                    .parentId(id1)
                                    .rootId(id1)
                                    .fileId("SomeFile2")
                                    .text("SomeTEXT2")
                                    .html("<some>html2</some>")
                                    .build()
                    ))
                    .build();
            String a1 = new ObjectMapper().writeValueAsString(son1);
            producer.send(new ProducerRecord<>(TOPIC, id1, a1)).get();

        } catch (ExecutionException | InterruptedException | JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
