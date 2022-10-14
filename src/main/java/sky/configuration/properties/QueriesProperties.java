package sky.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

// TODO: consider enabling moving this to some redis
@Data
@Configuration
public class QueriesProperties {
    @Bean("queues")
    @ConfigurationProperties("queues")
    public List<QueryProperties> createQueueProperties() {
        return new ArrayList<>();
    }
}
