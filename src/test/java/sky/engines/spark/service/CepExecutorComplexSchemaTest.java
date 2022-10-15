package sky.engines.spark.service;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import sky.configuration.debug.DebugDepthLevel;
import sky.configuration.debug.DebugMode;
import sky.configuration.debug.DebuggingAccuracyLevel;
import sky.model.DebugResult;
import sky.model.TargetSon;

import java.util.ArrayList;
import java.util.List;

@ExtendWith(SparkExtension.class)
class CepExecutorComplexSchemaTest {
    @SparkSessionField public SparkSession spark;
    @JavaSparkContextField public JavaSparkContext sparkContext;
    private StructType schema;
    private Dataset<Row> dataFrame;

    @BeforeEach
    void initializeDataframe() {
        schema = ExpressionEncoder.javaBean(TargetSon.class).schema();
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("ID1","ID1","Ou","47189473982148932194","1665863563896","[{SomeFile, <some>html</some>, Image, ID1, null, ID1, SomeTEXT}, {SomeFile2, <some>html2</some>, Zip, ID1, null, ID1, SomeTEXT2}, {SomeFile3, <some>html3</some>, Zip, ID1, null, ID1, SomeTEXT3}]"));
        dataFrame = spark.createDataFrame(rows, schema);
        dataFrame.createOrReplaceTempView("meteor");
    }

    private Expression getFilterCondition(String sqlStatement) {
        QueryExecution queryExecution = dataFrame.sqlContext().sql(sqlStatement).queryExecution();
        LogicalPlan analyzed = queryExecution.analyzed();
        Filter filter = (Filter)((Project) analyzed).child();
        return filter.condition();
    }

    @ParameterizedTest(name = "{index} ==> the debug process of the SQL statement ''{0}'' results detection of {1} as problematic when debugging failure")
    @CsvSource({
            "SELECT * FROM meteor WHERE hopId=\"Ou\" AND (to_timestamp(createdDate)<(current_date - INTERVAL 31 MINUTE)" +
                "OR to_timestamp(createdDate)>(current_date - INTERVAL 50 MINUTE))"
    })
    @DisplayName("Given a statement that failed with two leaf statements failures, while one of them inside an OR. Expect the debug result to return only the problematic one.")
    @Disabled
    void givenStatementOfAndClauses_whenDebuggingFailure_thenExpectTheProblematicAndStatementToReturn(String sqlStatement, String problematicQuery) {
        List<DebugResult> debugResults = new ArrayList<>();
        new QueriesInvestigator().investigateQueryExecution(new QueryInvestigation(debugResults,
                dataFrame,
                DebugMode.ON_FAILURE,
                DebuggingAccuracyLevel.FIND_KEY_STATEMENT,
                DebugDepthLevel.LEAF),
                getFilterCondition(sqlStatement));
        Assertions.assertThat(debugResults)
                .hasSize(1)
                .anyMatch(debugResult -> debugResult.getSql().equals(problematicQuery));
    }
}