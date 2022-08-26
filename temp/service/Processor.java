package sky.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import sky.config.S3ClientConfigurationProperties;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@AllArgsConstructor
@Slf4j
public class Processor implements ApplicationRunner {
    public static final int MAX_SIZE = 1000;
    public static final Duration MAX_TIME = Duration.ofSeconds(5);
    public static final String SOURCE_HEADER_NAME = "source";
    public static final String STAR = "STAR";
    public static final String ENRICHMENTS_TOPIC = "enrichments-topic";
    public static final String MESSI_TOPIC = "messi-topic";
    public ReceiverOptions<String, String> receiverOptions;
    public S3AsyncClient s3client;
    public S3ClientConfigurationProperties s3ClientConfigurationProperties;
    public ReactiveKafkaProducerTemplate reactiveKafkaProducerTemplate;
    public ConversionServiceAdapter adapter;
    public ReactiveMongoTemplate reactiveMongoTemplate;
    public String bucketName = "fajifjaie";
    ObjectMapper mapper = new ObjectMapper();

    private Optional<Message<ExampleData>> getObject(ReceiverRecord<String, String> receiverRecord) {
        try {
            ExampleData payload = mapper.readValue(receiverRecord.value(), ExampleData.class);
            return Optional.of(MessageBuilder
                    .withPayload(payload)
                    .setHeader("item_id", payload.getItemId())
                    .setHeader(SOURCE_HEADER_NAME, receiverRecord.headers().headers("source"))
                    .setHeader("receiverRecord", receiverRecord)
                    .build());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    @Bean
    @ServiceActivator(inputChannel = "inputChannel")
    public AbstractReplyProducingMessageHandler fileWritingMessageHandler() {
        AbstractReplyProducingMessageHandler mh = new AbstractReplyProducingMessageHandler() {
            @Override
            protected Object handleRequestMessage(Message<?> message) {
                String payload = (String) message.getPayload();
                return "Message Payload : ".concat(payload);
            }
        };
        mh.setOutputChannelName("outputChannel");
        return mh;
    }

    private IntegrationFlow bulkWriteToPulsar() {
        return null;
    }

    private IntegrationFlow bulkWriteToCockroach() {
        return null;
    }

    @Override
    public void run(ApplicationArguments args) {
        Function<Message<?>, String> idToGroupBy =
                message -> Stream.of(Objects.requireNonNull(message.getHeaders().getId()).toString()).collect(Collectors.joining());
        Flux.from(KafkaReceiver.create(receiverOptions)
                        .receive()
                        .map(this::getObject)
                        .map(Optional::get)
                        .onErrorContinue((err, msg) -> err.printStackTrace())
                        .flatMap(this::enrichFromS3)
//                .flatMap(objectMessageGroupedFlux -> objectMessageGroupedFlux
//                        .sort(Comparator.comparing(obj -> ((ExampleData) (obj.getPayload())).getReceptionTime()))
//                        .take(1))
                        .flatMap(this::iterativelyWriteToS3)
                        .groupBy(idToGroupBy)
                        .bufferTimeout(MAX_SIZE, MAX_TIME)
                        .flatMap(this::distinctIdsGroupStrategy)
                        .flatMap(this::writeToMongo)
//                        .flatMap(this::writeToSolr)
//                        .flatMap(this::executeGroupingMechanism)
                        .flatMap(Flux::fromIterable)
                        .flatMap(this::exampleEventMapper)
                        .flatMap(msg -> writeToKafka(ENRICHMENTS_TOPIC, msg))
                        .flatMap(this::commitToKafka)
                        .filter(message -> !this.enrichmentMessagesFilter(message))
                        .flatMap(msg -> writeToKafka(MESSI_TOPIC, msg))
                        .flatMap(this::commitToKafka)
        ).subscribe();
    }

    private Mono<List<Message<ExampleData>>> writeToMongo(List<Message<ExampleData>> messages) {
        return reactiveMongoTemplate.getCollection("assets_refs").flatMap(mongoCollection -> {
            List<UpdateOneModel<ExampleData>> operations = messages.stream().map(exampleDataMessage -> {
                Document doc = new Document();
                reactiveMongoTemplate.getConverter().write(exampleDataMessage, doc);
                var filter = getMongoUpsertCondition(exampleDataMessage);
                return new UpdateOneModel<ExampleData>(filter, new Document("$set", doc), new UpdateOptions().upsert(true));
            }).toList();
            return Mono.from(mongoCollection.bulkWrite(operations))
                    .then(Mono.just(messages));
        });
    }

    @NotNull
    private Document getMongoUpsertCondition(Message<ExampleData> exampleDataMessage) {
        return new Document("externalId", exampleDataMessage.getPayload().getItemId());
    }

    private Flux<List<Message<ExampleData>>> distinctIdsGroupStrategy(List<GroupedFlux<String, Message<ExampleData>>> groupedFluxes) {
        int longestList = 0;
        Iterable<List<Message<ExampleData>>> iterable = Flux.fromIterable(groupedFluxes)
                .flatMap(messageGroupedFlux -> messageGroupedFlux.bufferTimeout(MAX_SIZE, MAX_TIME))
                .toIterable();
        for (var messageList: iterable) {
            if (messageList.size() > longestList) {
                longestList = messageList.size();
            }
        }
        List<List<Message<ExampleData>>> listOfDistinctIdsList = new ArrayList<>();
        for (int i = 0; i < longestList; i++) {
            List<Message<ExampleData>> perPositionMessagesList = new ArrayList<>();
            for (var messageList: iterable) {
                perPositionMessagesList.add(messageList.get(i));
            }
            listOfDistinctIdsList.add(perPositionMessagesList);
        }
        return Flux.fromIterable(listOfDistinctIdsList);
    }

    private Flux<List<Message<ExampleData>>> distinctIdsGroupStrategyFunctionalImplementation(List<GroupedFlux<String, Message<ExampleData>>> groupedFluxes) {
        AtomicInteger longestList = new AtomicInteger();
        List<List<Message<ExampleData>>> listOfDistinctIdsList = new ArrayList<>();
        var flux = Flux.fromIterable(groupedFluxes)
                .flatMap(messageGroupedFlux -> messageGroupedFlux.bufferTimeout(MAX_SIZE, MAX_TIME))
                .flatMap(messageList -> {
                    if (messageList.size() > longestList.get()) {
                        longestList.set(messageList.size());
                    }
                    return Mono.just(messageList);
                });
        return Flux.range(0, longestList.get())
                .zipWith(flux)
                .flatMap(integerListTuple2 -> {
                    List<Message<ExampleData>> perPositionMessagesList = new ArrayList<>();
                    perPositionMessagesList.add(integerListTuple2.getT2().get(integerListTuple2.getT1()));
                    listOfDistinctIdsList.add(perPositionMessagesList);
                    return Mono.just(integerListTuple2.getT1());
                })
                .thenMany(Flux.fromIterable(listOfDistinctIdsList));
    }

    private Mono<Message<ExampleEvent>> writeToKafka(String topic, Message<ExampleEvent> exampleDataMessage) {
        return reactiveKafkaProducerTemplate.send(topic, exampleDataMessage.getHeaders().getId(), exampleDataMessage.getPayload())
                .thenReturn(exampleDataMessage);
    }

    private Mono<Message<ExampleEvent>> exampleEventMapper(Message<ExampleData> message) {
        return Mono.just(MessageBuilder
                        .withPayload(adapter.mapExampleDataToExampleEvent(message.getPayload()))
                        .copyHeaders(message.getHeaders())
                        .build());
    }

    private Mono<Message<ExampleData>> enrichFromS3(Message<ExampleData> message) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket("%s%d".formatted(s3ClientConfigurationProperties.getBucketPrefix(),
                        S3Partitioner.getPartition(message.getPayload().getItemId().getBytes(StandardCharsets.UTF_8), s3ClientConfigurationProperties.getNumPartitions())))
                .key(message.getPayload().getItemId())
                .build();
        return Mono.fromFuture(s3client.getObject(request, AsyncResponseTransformer.toBytes()))
                .doOnNext(getObjectResponse -> log.info(getObjectResponse.response().toString()))
                .then(Mono.just(message));
    }

    private Mono<Message<ExampleEvent>> commitToKafka(Message<ExampleEvent> message) {
        return Mono.just(message)
                .doOnNext(a -> {
                    if (enrichmentMessagesFilter(message)) {
                        ((ReceiverRecord<?, ?>) Objects.requireNonNull(message.getHeaders().get("receiverRecord")))
                                .receiverOffset().offset();
                    }
                });
    }

    private boolean enrichmentMessagesFilter(Message<ExampleEvent> message) {
        return Objects.equals(message.getHeaders().get(SOURCE_HEADER_NAME), STAR);
    }

    private Flux<Message<ExampleData>> executeGroupingMechanism(GroupedFlux<String, Message<ExampleData>> uuidMessageGroupedFlux) {
        return uuidMessageGroupedFlux
                .bufferTimeout(MAX_SIZE, MAX_TIME)
                .map(this::activatePolicy);
    }

    private Message<ExampleData> activatePolicy(List<Message<ExampleData>> messages) {
        return null;
    }

    private Publisher<Message<ExampleData>> iterativelyWriteToS3(Message<ExampleData> message) {
        return Flux.fromIterable(message.getPayload().getFiles())
                .flatMap(file -> writeToS3(MessageBuilder.withPayload(((File) file).getText()).build()))
                .doOnError(e -> log.error(e.getMessage()))
                .then(Mono.just(message));
    }

    private Publisher<Message<String>> writeToS3(Message<String> message) {
        return new ReactiveS3MessageHandler(s3client,
                new LiteralExpression(String.format("meteor-prod-text3-%s", message.getHeaders().getId())))
                .handleMessageInternal(message);
    }
}
