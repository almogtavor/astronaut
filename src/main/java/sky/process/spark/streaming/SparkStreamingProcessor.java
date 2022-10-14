package sky.process.spark.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import sky.configuration.properties.KafkaConsumerProperties;
import sky.configuration.properties.SinkType;
import sky.engines.spark.service.SparkCepExecutor;

import static org.apache.spark.sql.functions.*;

@Component
@AllArgsConstructor
@ConditionalOnProperty(prefix = "astronaut.runner", name = "technology", havingValue = "spark-streaming")
@Slf4j
public class SparkStreamingProcessor implements ApplicationRunner {
    private final SparkSession spark;
    private final SparkCepExecutor cepExecutor;
    private final KafkaConsumerProperties kafkaConsumerProperties;
    private final ObjectMapper objectMapper;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("Initializing Spark Structured Streaming application runner.");
        StructType structType = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, false, null),
                new StructField("age", DataTypes.IntegerType, false, null),
                new StructField("job", DataTypes.IntegerType, false, null)
        });
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", String.join(",", kafkaConsumerProperties.getBootstrapServers()))
                .option("subscribe", kafkaConsumerProperties.getTopic())
                .option("startingOffsets", kafkaConsumerProperties.getConsumer().getAutoOffsetReset())
                .load();
        df = df.select(from_json(col("value"), structType),
                col("kafka_key")
                );
        cepExecutor.executeSqlStatements(df, SinkType.STREAMING);
    }
}
