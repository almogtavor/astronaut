package sky.engines.spark.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import scala.reflect.io.File;
import sky.configuration.properties.KafkaProducersProperties;
import sky.configuration.properties.QueryProperties;
import sky.configuration.properties.QueueTechnology;
import sky.configuration.properties.SinkType;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.lit;

@Service
@AllArgsConstructor
@Slf4j
public class SparkQueuesPublisher {
    @Qualifier("kafkaProducersPropertiesMap")
    private final Map<String, KafkaProducersProperties.KafkaProducerProperties> kafkaProducersProperties;

    public Dataset<Row> prepareResultsForPublishing(QueryProperties queryProperties, Dataset<Row> results) {
        if (queryProperties.getQueueTechnology().equals(QueueTechnology.KAFKA)) {
            return results.select(to_json(struct("*")).as("value"))
                    .withColumn("headers",
                            array(
                                    struct(lit(QueryProperties.Fields.queryName).as("key"), lit(queryProperties.getQueryName()).as("value")),
                                    struct(lit(QueryProperties.Fields.sqlQuery).as("key"), lit(queryProperties.getSqlQuery()).as("value"))
                            ));
        }
        return results;
    }

    public void publishMessagesToQueues(SinkType sinkType, QueryProperties queryProperties, Dataset<Row> kafkaDataset) {
        if (queryProperties.getQueueTechnology().equals(QueueTechnology.KAFKA)) {
            KafkaProducersProperties.KafkaProducerProperties kafkaProducerProperties = kafkaProducersProperties.get(queryProperties.getQueueName());
            if (sinkType.equals(SinkType.SPARK_BATCH_METHOD)) {
                kafkaDataset
                        .selectExpr("CAST(value AS BINARY) value")
                        .write()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", String.join(",", kafkaProducerProperties.getBootstrapServers()))
                        .option("topic", kafkaProducerProperties.getTopic())
                        .save();
            } else {
                try {
                    StreamingQuery ds = kafkaDataset
                            .selectExpr("CAST(value AS BINARY) value")
                            .writeStream()
                            .format("kafka")
                            .option("kafka.bootstrap.servers", String.join(",", kafkaProducerProperties.getBootstrapServers()))
                            .option("topic", kafkaProducerProperties.getTopic())
                            .option("checkpointLocation", System.getProperty("user.home") + File.pathSeparator() + ".checkpoint")
                            .start();
                    ds.awaitTermination();
                } catch (TimeoutException | StreamingQueryException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
