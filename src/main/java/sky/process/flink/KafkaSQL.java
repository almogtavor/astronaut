package sky.process.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

public class KafkaSQL {

    public static void main(String[] args) throws Exception {

        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create table environment
        TableEnvironment tableEnv = TableEnvironment.create(env);

        // set up properties for Kafka consumer
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", "localhost:9092");
        consumerProperties.setProperty("group.id", "flink-consumer");

        // create Kafka consumer
        FlinkKafkaConsumer<MyObject> consumer = new FlinkKafkaConsumer<>("topic", new MyDeserializationSchema(), consumerProperties);

        // set up properties for Kafka producer
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "localhost:9092");

        // create Kafka producer
        FlinkKafkaProducer<MyObject> producer = new FlinkKafkaProducer<>("output-topic", new MySerializationSchema(), producerProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        // create a DataStream from the Kafka consumer
        DataStream<MyObject> inputStream = env.addSource(consumer);

        // register the DataStream as a table in the table environment
        Table inputTable = tableEnv.fromDataStream(inputStream, "field1, field2, field3");

        // execute a SQL query on the table
        Table resultTable = tableEnv.sqlQuery("SELECT field1, field2 FROM inputTable WHERE field3 > 10");

        // convert the result table back to a DataStream and write it to Kafka using the producer
        DataStream<MyObject> outputStream = tableEnv.toAppendStream(resultTable, MyObject.class);
        outputStream.addSink(producer);

        // execute the program
        env.execute("Kafka SQL");
    }

    // define a custom serialization schema for the Kafka producer
//    public static class MySerializationSchema implements SerializationSchema<
}