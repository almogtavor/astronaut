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

    public Mono<List<Message<String>>> wrapMessagesSqlExecutionWithReactiveStreams(List<Message<String>> messages) {
        try {
            var df = spark.read()
                    .json(spark.createDataset(messages.stream().map(Message::getPayload).toList(), Encoders.STRING()));
            cepExecutor.executeSqlStatements(df, SinkType.BATCH);
            return Mono.just(messages);
        } catch (Exception e) {
            log.error("An unhandled error occurred during SQL statement execution", e);
            return Mono.empty();
        }
    }
    public Mono<Message<String>> wrapMessageSqlExecutionWithReactiveStreams(Message<String> message) {
        try {
            var df = spark.read()
                    .json(spark.createDataset(List.of(message.getPayload()), Encoders.STRING()));
            cepExecutor.executeSqlStatements(df, SinkType.BATCH);
            return Mono.just(message);
        } catch (Exception e) {
            log.error("An unhandled error occurred during SQL statement execution", e);
            return Mono.empty();
        }
    }

}
