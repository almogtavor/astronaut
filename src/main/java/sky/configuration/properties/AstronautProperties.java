package sky.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import sky.configuration.debug.DebugMode;

@ConfigurationProperties("astronaut")
@Configuration
@Data
public class AstronautProperties {
    private DebugMode debugMode;
    private boolean debugEnabled;
}
