package sky.configuration;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FlinkConfiguration {
    @Bean
    public SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .config("spark.master", "local")
                .appName("Java Spark SQL astronaut application")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
    }
}
