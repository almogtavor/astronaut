package sky.config;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.sender.SenderOptions;

@Configuration
public class ReactiveKafkaProducerConfig {
//    @Bean
//    ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate(KafkaProperties kafkaProperties) {
//        Map<String, Object> props = kafkaProperties.buildProducerProperties();
//        return new ReactiveKafkaProducerTemplate<>(SenderOptions.create(props));
//    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate(SenderOptions<String, String> senderOptions) {
        return new ReactiveKafkaProducerTemplate<>(senderOptions
            .withKeySerializer(new StringSerializer())
            .withValueSerializer(new StringSerializer()));
    }
}
