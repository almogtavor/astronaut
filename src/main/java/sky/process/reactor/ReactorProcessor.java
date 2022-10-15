package sky.process.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import sky.configuration.properties.KafkaConsumerProperties;

import java.util.Map;
import java.util.Objects;

@Slf4j
@Component
@AllArgsConstructor
@ConditionalOnProperty(prefix = "astronaut.runner", name = "technology", havingValue = "reactor")
public class ReactorProcessor implements ApplicationRunner {
    public static final String RECEIVER_OFFSET_HEADER = "receiverOffset";
    private final ReactorSparkCepWrapper reactorSparkWrapper;
    private final KafkaReceiver<String, String> kafkaReceiver;
    private final KafkaConsumerProperties kafkaConsumerProperties;
    private final ObjectMapper objectMapper;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("Initializing Project Reactor application runner.");

        kafkaReceiver.receive()
                .map(msg -> MessageBuilder.createMessage(msg.value(),
                        new MessageHeaders(Map.of(RECEIVER_OFFSET_HEADER, msg.receiverOffset()))))
                .flatMap((Message<String> message) -> reactorSparkWrapper.executeSqlStatements(message, kafkaConsumerProperties.getTableName()))
                .mapNotNull(this::acknowledgeMessages)
                .subscribe();
    }

    @NotNull
    private Message<String> acknowledgeMessages(Message<String> msg) {
        try {
            ((ReceiverOffset) Objects.requireNonNull(msg.getHeaders().get(RECEIVER_OFFSET_HEADER))).acknowledge();
            return msg;
        } catch (Exception e) {
            log.error("An unhandled error occurred during SQL statement execution", e);
            return null;
        }
    }
}
