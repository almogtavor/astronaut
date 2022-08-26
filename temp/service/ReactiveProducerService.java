package sky.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Service;


@Service
public class ReactiveProducerService {
    private final ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate;

    @Qualifier("directChannel")
    @Autowired
    public MessageChannel directChannel;

    public ReactiveProducerService(ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate) {
        this.reactiveKafkaProducerTemplate = reactiveKafkaProducerTemplate;
    }

    @Bean
    public IntegrationFlow kafkaProducerFlow() {
        return IntegrationFlows.from(directChannel)
            .handle(s -> reactiveKafkaProducerTemplate.send("topic2", s.getPayload().toString()).subscribe(System.out::println))
            .get();
    }
}
