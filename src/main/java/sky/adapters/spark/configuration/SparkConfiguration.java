package sky.adapters.spark.configuration;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;

@Configuration
public class SparkConfiguration {
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
