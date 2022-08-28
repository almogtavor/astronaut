package sky.configuration.properties;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@ConfigurationProperties("astronaut")
@Configuration
@Data
public class AstronautProperties {
    private DebugMode debugMode;

//    @AllArgsConstructor
//    @Data
//    public static class DebugQuery {
//        private DebugMode debugMode;
//    }
}
