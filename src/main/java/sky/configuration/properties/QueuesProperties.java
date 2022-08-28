package sky.configuration.properties;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

// TODO: consider enabling moving this to some redis
@ConfigurationProperties("queues")
@Data
public class QueuesProperties {
    private List<QueueProperties> queueProperties;
}
