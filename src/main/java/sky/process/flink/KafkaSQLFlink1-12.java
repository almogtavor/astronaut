//package sky.process.flink;
//
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.TableEnvironment;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.table.descriptors.Json;
//import org.apache.flink.table.descriptors.Kafka;
//import org.apache.flink.table.descriptors.Rowtime;
//import org.apache.flink.table.descriptors.Schema;
//
//public class KaxfkaSQL {
//
//    public static void main(String[] args) throws Exception {
//        // parse input arguments
//        final ParameterTool params = ParameterTool.fromArgs(args);
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
//
//        // configure Kafka consumer
//        Kafka kafka = new Kafka()
//                .version("universal")
//                .topic("my-topic")
//                .property("bootstrap.servers", "localhost:9092")
//                .property("group.id", "my-group");
//
//        // configure the table schema
//        Schema schema = new Schema()
//                .field("timestamp", Types.SQL_TIMESTAMP)
//                .rowtime(new Rowtime().timestampsFromField("timestamp"))
//                .field("name", Types.STRING)
//                .field("age", Types.INT);
//
//        // register the Kafka table
//        tEnv.connect(kafka)
//                .withFormat(new Json().failOnMissingField(true))
//                .withSchema(schema)
//                .inAppendMode()
//                .registerTableSource("users");
//
//        // perform a simple string-based SQL query
//        Table result = tEnv.sqlQuery("SELECT name, age FROM users WHERE age > 30");
//
//        tEnv.toAppendStream(result, Row.class).print();
//
//        env.execute();
//    }
//}
