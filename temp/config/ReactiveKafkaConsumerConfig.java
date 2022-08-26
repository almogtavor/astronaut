package sky.config;

//import reactor.kafka.spring.integration.samples.dto.ConsumerPojo;
//import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.messaging.MessageChannel;
import reactor.kafka.receiver.ReceiverOptions;

//import java.util.Collections;

@Configuration
public class ReactiveKafkaConsumerConfig {
//    @Bean
//    ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate(KafkaProperties kafkaProperties) {
//        return new ReactiveKafkaConsumerTemplate<>(
//            ReceiverOptions.<String, String>create(kafkaProperties.buildConsumerProperties())
//                .subscription(Collections.singleton("Topic1")));
//    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate(ReceiverOptions<String, String> receiverOptions) {
        return new ReactiveKafkaConsumerTemplate<>(receiverOptions
            .withKeyDeserializer(new StringDeserializer())
            .withValueDeserializer(new StringDeserializer()));
    }
}








