package sky.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Service;


@Service
public class ReactiveConsumerService {
    public ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate;

    public ReactiveConsumerService(ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate) {
        this.reactiveKafkaConsumerTemplate = reactiveKafkaConsumerTemplate;
    }

    @Bean
    public IntegrationFlow readFromKafka() {
        return IntegrationFlows.from(reactiveKafkaConsumerTemplate.receiveAutoAck()
                .map(GenericMessage::new))
            .<ConsumerRecord<String, String>, String>transform(ConsumerRecord::value)
            .<String, String>transform(String::toUpperCase)
            .channel(directChannel)
            .get();
    }
}
