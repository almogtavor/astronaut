package sky.process.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import sky.configuration.properties.KafkaConsumerProperties;
import sky.configuration.properties.SinkType;
import sky.engines.spark.service.SparkCepExecutor;
import sky.model.TargetSon;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

@Component
@AllArgsConstructor
@ConditionalOnProperty(prefix = "astronaut.runner", name = "technology", havingValue = "spark-streaming")
@Slf4j
public class FlinkProcessor implements ApplicationRunner {
    private final SparkSession spark;
    private final SparkCepExecutor cepExecutor;
    private final KafkaConsumerProperties kafkaConsumerProperties;
    private final ObjectMapper objectMapper;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("Initializing Flink application runner.");
        var schema = ExpressionEncoder.javaBean(TargetSon.class).schema();
        System.out.println(schema.json());
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", String.join(",", kafkaConsumerProperties.getBootstrapServers()))
                .option("subscribe", kafkaConsumerProperties.getTopic())
                .option("startingOffsets", kafkaConsumerProperties.getConsumer().getAutoOffsetReset())
                .load();
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("brokers")
                .setTopics("input-topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        df.createOrReplaceTempView("meteor");
        df = df
                .select(Arrays.stream(df.columns()).map(c -> col(c).cast(DataTypes.StringType).alias(c)).toArray(Column[]::new))
                .select(from_json(col("value"), schema).alias("values"), col("key"), col("timestamp"));
        df = df.select("values.*", "key", "timestamp");
        df = df.withColumnRenamed("createdDate", "createdDateTemp");
        df = df.withColumn("createdDate", to_timestamp(
                concat_ws("/", col("createdDateTemp.year"), col("createdDateTemp.month"), col("createdDateTemp.date"), col("createdDateTemp.hours"), col("createdDateTemp.minutes"), col("createdDateTemp.seconds")),
                "yyyy/MM/dd/HH/mm/SSS"));
        df = df.drop("createdDateTemp");
        df.printSchema();
        df
                .writeStream()
                .format("console")
                .option("truncate","false")
                .start();
        df.createOrReplaceTempView(kafkaConsumerProperties.getTableName());
        cepExecutor.executeSqlStatements(SinkType.SPARK_STREAMING_METHOD);
    }
}
