package sky.adapters.inbound;//package reactor.kafka.spring.integration.samples.channel.adapters.inbound;
//
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.integration.endpoint.MessageProducerSupport;
//import org.springframework.integration.handler.AbstractReactiveMessageHandler;
//import org.springframework.messaging.Message;
//import reactor.core.publisher.Mono;
//import reactor.kafka.spring.integration.samples.config.S3ClientConfigurationProperties;
//import software.amazon.awssdk.core.async.AsyncRequestBody;
//import software.amazon.awssdk.services.s3.S3AsyncClient;
//import software.amazon.awssdk.services.s3.model.PutObjectRequest;
//import software.amazon.awssdk.services.s3.model.PutObjectResponse;
//
//import java.nio.ByteBuffer;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.UUID;
//import java.util.concurrent.CompletableFuture;
//
//@Slf4j
//public class ReactiveKafkaMessageProducer extends MessageProducerSupport {
//    private final S3AsyncClient s3client;
//    private final S3ClientConfigurationProperties s3config;
//
//    public ReactiveKafkaMessageProducer(S3AsyncClient s3client, S3ClientConfigurationProperties s3config) {
//        this.s3client = s3client;
//        this.s3config = s3config;
//    }
//    private final ReactiveMongoOperations mongoOperations;
//
//    private Class<?> domainType = Document.class;
//
//    @Nullable
//    private String collection;
//
//    private ChangeStreamOptions options = ChangeStreamOptions.empty();
//
//    private boolean extractBody = true;
//
//    /**
//     * Create an instance based on the provided {@link ReactiveMongoOperations}.
//     * @param mongoOperations the {@link ReactiveMongoOperations} to use.
//     * @see ReactiveMongoOperations#changeStream(String, ChangeStreamOptions, Class)
//     */
//    public MongoDbChangeStreamMessageProducer(ReactiveMongoOperations mongoOperations) {
//        Assert.notNull(mongoOperations, "'mongoOperations' must not be null");
//        this.mongoOperations = mongoOperations;
//    }
//
//    /**
//     * Specify an object type to convert an event body to.
//     * Defaults to {@link Document} class.
//     * @param domainType the class for event body conversion.
//     * @see ReactiveMongoOperations#changeStream(String, ChangeStreamOptions, Class)
//     */
//    public void setDomainType(Class<?> domainType) {
//        Assert.notNull(domainType, "'domainType' must not be null");
//        this.domainType = domainType;
//    }
//
//    /**
//     * Specify a collection name to track change events from.
//     * By default tracks all the collection in the {@link #mongoOperations} configured database.
//     * @param collection a collection to use.
//     * @see ReactiveMongoOperations#changeStream(String, ChangeStreamOptions, Class)
//     */
//    public void setCollection(String collection) {
//        this.collection = collection;
//    }
//
//    /**
//     * Specify a {@link ChangeStreamOptions}.
//     * @param options the {@link ChangeStreamOptions} to use.
//     * @see ReactiveMongoOperations#changeStream(String, ChangeStreamOptions, Class)
//     */
//    public void setOptions(ChangeStreamOptions options) {
//        Assert.notNull(options, "'options' must not be null");
//        this.options = options;
//    }
//
//    /**
//     * Configure this channel adapter to build a {@link Message} to produce
//     * with a payload based on a {@link org.springframework.data.mongodb.core.ChangeStreamEvent#getBody()} (by default)
//     * or use a whole {@link org.springframework.data.mongodb.core.ChangeStreamEvent} as a payload.
//     * @param extractBody to extract {@link org.springframework.data.mongodb.core.ChangeStreamEvent#getBody()} or not.
//     */
//    public void setExtractBody(boolean extractBody) {
//        this.extractBody = extractBody;
//    }
//
//    @Override
//    public String getComponentType() {
//        return "mongo:change-stream-inbound-channel-adapter";
//    }
//
//    @Override
//    protected void doStart() {
//        Flux<Message<?>> changeStreamFlux =
//                this.mongoOperations.changeStream(this.collection, this.options, this.domainType)
//                        .map(event ->
//                                MessageBuilder
//                                        .withPayload(
//                                                !this.extractBody || event.getBody() == null
//                                                        ? event
//                                                        : event.getBody())
//                                        .setHeader(MongoHeaders.COLLECTION_NAME, event.getCollectionName())
//                                        .setHeader(MongoHeaders.CHANGE_STREAM_OPERATION_TYPE, event.getOperationType())
//                                        .setHeader(MongoHeaders.CHANGE_STREAM_TIMESTAMP, event.getTimestamp())
//                                        .setHeader(MongoHeaders.CHANGE_STREAM_RESUME_TOKEN, event.getResumeToken())
//                                        .build());
//
//        subscribeToPublisher(changeStreamFlux);
//    }
//
//
//}
