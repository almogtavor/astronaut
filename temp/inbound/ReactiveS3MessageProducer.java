package sky.adapters.inbound;//package reactor.kafka.spring.integration.samples.channel.adapters.inbound;
//
//import lombok.extern.slf4j.Slf4j;
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
//public class ReactiveS3MessageProducer extends AbstractReactiveMessageHandler {
//    private final S3AsyncClient s3client;
//    private final S3ClientConfigurationProperties s3config;
//
//    public ReactiveS3MessageProducer(S3AsyncClient s3client, S3ClientConfigurationProperties s3config) {
//        this.s3client = s3client;
//        this.s3config = s3config;
//    }
//
//    @Override
//    protected Mono<Void> handleMessageInternal(Message<?> message) {
////        long length = message.getHeaders().getl();
////        if (length < 0) {
////            throw new UploadFailedException(HttpStatus.BAD_REQUEST.value(), Optional.of("required header missing: Content-Length"));
////        }
//
//        String fileKey = UUID.randomUUID().toString();
//        Map<String, String> metadata = new HashMap<String, String>();
////        MediaType mediaType = headers.getContentType();
////
////        if (mediaType == null) {
////            mediaType = MediaType.APPLICATION_OCTET_STREAM;
////        }
//
////        log.info("[I95] uploadHandler: mediaType{}, length={}", mediaType, length);
//        ByteBuffer wrap = ByteBuffer.wrap(message.getPayload().toString().getBytes());
//        CompletableFuture<PutObjectResponse> future = s3client
//                .putObject(PutObjectRequest.builder()
//                                .bucket(s3config.getBucket())
////                                .contentLength(length)
//                                .key(fileKey.toString())
////                                .contentType(mediaType.toString())
//                                .metadata(metadata)
//                                .build(),
//                        AsyncRequestBody.fromPublisher(Mono.just(wrap)));
//
//        return Mono.fromFuture(future).then();
//    }
//}
