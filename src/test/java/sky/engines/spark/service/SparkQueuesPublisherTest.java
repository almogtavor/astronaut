package sky.engines.spark.service;

//import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import sky.configuration.properties.QueryProperties;
import sky.configuration.properties.QueueTechnology;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(SparkExtension.class)
class SparkQueuesPublisherTest {
    @SparkSessionField public SparkSession spark;
    @JavaSparkContextField public JavaSparkContext sparkContext;
    private StructType schema;
    private Dataset<Row> dataFrame;

    @Test
    void prepareResultsForPublishing() {
        schema = DataTypes.createStructType(List.of(
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.StringType, true),
                DataTypes.createStructField("job", DataTypes.StringType, true)));
        List<Row> nums = new ArrayList<>();
        nums.add(RowFactory.create("George","30","Developer"));
        dataFrame = spark.createDataFrame(nums, schema);
        dataFrame.createOrReplaceTempView("meteor");

        String queryName = "someQueryName";
        String sqlQuery = "SELECT * FROM meteor WHERE hopId=\"Ou\" AND (createdDate<31 OR createdDate>50)";
        var queryProperties = QueryProperties.builder()
                .sqlQuery(sqlQuery)
                .queueTechnology(QueueTechnology.KAFKA)
                .queueName("someQueueName")
                .queryName(queryName)
                .build();

        SparkQueuesPublisher sparkQueuesPublisher = Mockito.mock(SparkQueuesPublisher.class);
        when(sparkQueuesPublisher.prepareResultsForPublishing(queryProperties, dataFrame)).thenCallRealMethod();
        Dataset<Row> resultsForPublishing = sparkQueuesPublisher.prepareResultsForPublishing(queryProperties, dataFrame);
        var value = resultsForPublishing.select("value").collectAsList().get(0).toString();
        var headers = resultsForPublishing.select("headers").collectAsList().get(0).toString();
        assertThat(value)
                .isEqualTo("[{\"name\":\"George\",\"age\":\"30\",\"job\":\"Developer\"}]");
        assertThat(headers)
                .isEqualTo("[ArraySeq([queryName,%s], [sqlQuery,%s])]".formatted(queryName, sqlQuery));
    }
}