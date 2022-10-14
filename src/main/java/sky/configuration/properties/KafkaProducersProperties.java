package sky.configuration.properties;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Data
@Configuration
public class KafkaProducersProperties {
    @Bean("kafkaProducersPropertiesMap")
    @ConfigurationProperties("spring.kafka.producers-env")
    public Map<String, KafkaProducerProperties> createKafkaProducersPropertiesMap() {
        return new HashMap<>();
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class KafkaProducerProperties extends KafkaProperties {
        private String topic;
    }
}
