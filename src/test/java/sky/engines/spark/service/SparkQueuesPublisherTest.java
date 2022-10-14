//package sky.engines.spark.service;
//
////import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.*;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructType;
//import org.junit.Ignore;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.Mockito;
//import sky.configuration.properties.QueryProperties;
//import sky.configuration.properties.QueueTechnology;
//
//import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.List;
//
//import static org.apache.spark.sql.functions.*;
//import static org.junit.jupiter.api.Assertions.*;
//import static org.mockito.Mockito.when;
//
//@ExtendWith(SparkExtension.class)
//class SparkQueuesPublisherTest extends JavaDataFrameSuiteBase {
//    @SparkSessionField public SparkSession spark;
//    @JavaSparkContextField public JavaSparkContext sparkContext;
//    private StructType schema;
//    private Dataset<Row> dataFrame;
//
//    @BeforeEach
//    void initializeDataframe() {
//        schema = DataTypes.createStructType(List.of(
//                DataTypes.createStructField("name", DataTypes.StringType, true),
//                DataTypes.createStructField("age", DataTypes.StringType, true),
//                DataTypes.createStructField("job", DataTypes.StringType, true)));
//        List<Row> nums = new ArrayList<>();
//        nums.add(RowFactory.create("George","30","Developer"));
//        dataFrame = spark.createDataFrame(nums, schema);
//        dataFrame.createOrReplaceTempView("meteor");
//    }
//
//    @Test
//    @Ignore
//    @Disabled
//    void prepareResultsForPublishing() {
////        schema = DataTypes.createStructType(List.of(
////                DataTypes.createStructField("value", DataTypes.StringType, true),
////                DataTypes.createStructField("headers", DataTypes.createArrayType(
////                        DataTypes.createStructType(List.of(
////                                DataTypes.createStructField("key", DataTypes.StringType, false),
////                                DataTypes.createStructField("value", DataTypes.StringType, true)
////                        )), false), false)));
//        var expectedSchema = DataTypes.createStructType(List.of(
//                DataTypes.createStructField("value", DataTypes.StringType, true),
//                DataTypes.createStructField("headers", DataTypes.StringType, false)));
//        List<Row> rows = new ArrayList<>();
//        rows.add(RowFactory.create("{\"name\":\"George\",\"age\":\"30\",\"job\":\"Developer\"}", "[{\"queryName\": \"someQueryName\"}, {\"sqlQuery\": \"null\"}]"));
//        var expectedDataFrame = spark.createDataFrame(rows, expectedSchema);
//        expectedDataFrame = expectedDataFrame.select(col("value"), from_json(col("headers"), DataTypes.createArrayType(
//                DataTypes.createStructType(List.of(
//                        DataTypes.createStructField("key", DataTypes.StringType, false),
//                        DataTypes.createStructField("value", DataTypes.StringType, true)
//                )), false)).as("headers"));
//
//        var queryProperties = QueryProperties.builder()
//                .queueTechnology(QueueTechnology.KAFKA)
//                .queueName("someQueueName")
//                .queryName("someQueryName")
//                .build();
//
//        SparkQueuesPublisher sparkQueuesPublisher = Mockito.mock(SparkQueuesPublisher.class);
//        when(sparkQueuesPublisher.prepareResultsForPublishing(queryProperties, dataFrame)).thenCallRealMethod();
//        expectedDataFrame.show(false);
//        sparkQueuesPublisher.prepareResultsForPublishing(queryProperties, dataFrame).show(false);
//        assertDataFrameEquals(expectedDataFrame, sparkQueuesPublisher.prepareResultsForPublishing(queryProperties, dataFrame));
//    }
//}