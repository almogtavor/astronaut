package sky.integration;

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

import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

//@ContextConfiguration(initializers = KafkaIntegrationTests.Initializer.class)
//@ExtendWith(SpringExtension.class)
//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Testcontainers(disabledWithoutDocker = true)
@Slf4j
@SpringBootTest(classes = AstronautApplication.class)
class KafkaIntegrationTests {
    public static final short REPLICATION_FACTOR = 1;
    public static final int NUM_PARTITIONS = 3;
    public static final String TOPIC = "test-topic";
    @Container
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
    //    private ReceiverOptions<Integer, String> receiverOptions;
//    private KafkaSender<Integer, String> sender;
    private final KafkaConsumerProperties kafkaConsumerProperties;

    public KafkaIntegrationTests(@Autowired KafkaConsumerProperties kafkaConsumerProperties) {
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
                            TOPIC));
        }
    }

//    @BeforeAll
//    public void startContainer() throws ExecutionException, InterruptedException, TimeoutException {
//
//    }

//    @Test
    @Disabled
    void testKafkaFunctionality() throws Exception {
        AdminClient adminClient = AdminClient.create(
                ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers())
        );

        Collection<NewTopic> topics = Collections.singletonList(new NewTopic(TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR));
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


        }
    }

}
