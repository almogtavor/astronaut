package sky.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.support.TestPropertySourceUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import sky.AstronautApplication;
import sky.configuration.properties.KafkaConsumerProperties;
import sky.model.TargetFile;
import sky.model.TargetSon;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

//@ContextConfiguration(initializers = KafkaIntegrationTests.Initializer.class)
//@ExtendWith(SpringExtension.class)
//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Testcontainers(disabledWithoutDocker = true)
@Slf4j
@SpringBootTest(classes = AstronautApplication.class)
class SqlIntegrationTests {
    public static final short REPLICATION_FACTOR = 1;
    public static final int NUM_PARTITIONS = 3;
    public static final String INPUT_TOPIC = "test-topic";
    public static final String OUTPUT_TOPIC = "output-topic";
    @Container
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
    //    private ReceiverOptions<Integer, String> receiverOptions;
//    private KafkaSender<Integer, String> sender;
    private final KafkaConsumerProperties kafkaConsumerProperties;

    public SqlIntegrationTests(@Autowired KafkaConsumerProperties kafkaConsumerProperties) {
        this.kafkaConsumerProperties = kafkaConsumerProperties;
    }

    static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(@NotNull ConfigurableApplicationContext configurableApplicationContext) {
            KAFKA_CONTAINER.start();

            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(configurableApplicationContext,
                    format("spring.kafka.consumer.bootstrap-servers=%s",
                            KAFKA_CONTAINER.getBootstrapServers()),
                    format("spring.kafka.consumer.topic=%s",
                            INPUT_TOPIC));
        }
    }

//    @Test
    @Disabled
    void testKafkaFunctionality() throws Exception {
        AdminClient adminClient = AdminClient.create(
                ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers())
        );

        Collection<NewTopic> topics = List.of(
                new NewTopic(INPUT_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR),
                new NewTopic(OUTPUT_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR)
        );
        adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
        try (
                KafkaProducer<String, String> producer = new KafkaProducer<>(
                        ImmutableMap.of(
                                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                KAFKA_CONTAINER.getBootstrapServers(),
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
                    .files(List.of(
                            TargetFile.builder()
                                    .parentId(id1)
                                    .fileId("SomeFile")
                                    .itemType("Image")
                                    .text("SomeTEXT")
                                    .html("<some>html</some>")
                                    .build(),
                            TargetFile.builder()
                                    .parentId(id1)
                                    .fileId("SomeFile2")
                                    .itemType("Zip")
                                    .text("SomeTEXT2")
                                    .html("<some>html2</some>")
                                    .build(),
                            TargetFile.builder()
                                    .parentId(id1)
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
                    .files(List.of(
                            TargetFile.builder()
                                    .parentId(id1)
                                    .fileId("SomeFile")
                                    .text("SomeTEXT")
                                    .html("<some>html</some>")
                                    .build(),
                            TargetFile.builder()
                                    .parentId(id1)
                                    .fileId("SomeFile2")
                                    .text("SomeTEXT2")
                                    .html("<some>html2</some>")
                                    .build()
                    ))
                    .build();
            String a1 = new ObjectMapper().writeValueAsString(son1);
            producer.send(new ProducerRecord<>(INPUT_TOPIC, id1, a1)).get();


        }
    }

}
