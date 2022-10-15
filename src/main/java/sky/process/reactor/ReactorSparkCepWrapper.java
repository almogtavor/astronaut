package sky.process.reactor;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import sky.configuration.properties.SinkType;
import sky.engines.spark.service.SparkCepExecutor;

import java.util.List;

@Service
@AllArgsConstructor
@Slf4j
public class ReactorSparkCepWrapper {
    private SparkSession spark;
    private SparkCepExecutor cepExecutor;

    public Mono<List<Message<String>>> executeSqlStatements(List<Message<String>> messages, String tableName) {
        try {
            var df = spark.read()
                    .json(spark.createDataset(messages.stream().map(Message::getPayload).toList(), Encoders.STRING()));
            df.createOrReplaceTempView(tableName);
            cepExecutor.executeSqlStatements(SinkType.SPARK_BATCH_METHOD);
            return Mono.just(messages);
        } catch (Exception e) {
            log.error("An unhandled error occurred during SQL statement execution", e);
            return Mono.empty();
        }
    }

    public Mono<Message<String>> executeSqlStatements(Message<String> message, String tableName) {
        try {
            var df = spark.read()
                    .json(spark.createDataset(List.of(message.getPayload()), Encoders.STRING()));
            df.createOrReplaceTempView(tableName);
            cepExecutor.executeSqlStatements(SinkType.SPARK_BATCH_METHOD);
            return Mono.just(message);
        } catch (Exception e) {
            log.error("An unhandled error occurred during SQL statement execution", e);
            return Mono.empty();
        }
    }
}
