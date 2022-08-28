package sky.adapters.spark.service;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.extension.ExtendWith;
import sky.configuration.properties.DebugDepthLevel;
import sky.configuration.properties.DebugMode;
import sky.configuration.properties.DebuggingAccuracyLevel;
import sky.model.DebugResult;

@ExtendWith(SparkExtension.class)
class CepExecutorTest {
    @SparkSessionField public SparkSession spark;
    @JavaSparkContextField public JavaSparkContext sparkContext;
    private StructType schema;
    private Dataset<Row> dataFrame;

    @BeforeEach
    void initializeDataframe() {
        schema = DataTypes.createStructType(List.of(
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.StringType, true),
                DataTypes.createStructField("job", DataTypes.StringType, true)));
        List<Row> nums = new ArrayList<>();
        nums.add(RowFactory.create("George","30","Developer"));
        dataFrame = spark.createDataFrame(nums, schema);
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
            "SELECT * FROM meteor WHERE name=\"Ou\" AND (age<31 OR age>50),(meteor.name = 'Ou')",
            "SELECT * FROM meteor WHERE name IS NULL,(meteor.name IS NULL)"
    })
    @DisplayName("Given a statement that failed with two leaf statements failures, while one of them inside an OR. Expect the debug result to return only the problematic one.")
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

    @ParameterizedTest(name = "{index} ==> the debug process of the SQL statement ''{0}'' results detection of {1} as problematic when debugging success")
    @CsvSource({
            "SELECT * FROM meteor WHERE name IS NOT NULL,(meteor.name IS NOT NULL)"
    })
    void givenStatementOfOneProblematicClause_whenDebuggingSuccess_thenExpectTheProblematicAndStatementToReturn(String sqlStatement, String problematicQuery) {
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

    @Test
    void givenStatementOfAndClauses_whenDebuggingSuccess_thenExpectTheTwoStatementsToReturn() {
        String sqlStatement = "SELECT * FROM meteor WHERE name=\"George\" AND (age<31 OR age>50)";
        List<DebugResult> debugResults = new ArrayList<>();
        new QueriesInvestigator().investigateQueryExecution(new QueryInvestigation(debugResults,
                dataFrame,
                DebugMode.ON_SUCCESS,
                DebuggingAccuracyLevel.FIND_KEY_STATEMENT,
                DebugDepthLevel.LEAF),
                getFilterCondition(sqlStatement));
        Assertions.assertThat(debugResults)
                .hasSize(2)
                .anyMatch(debugResult -> debugResult.getSql().equals("(meteor.name = 'George')"))
                .anyMatch(debugResult -> debugResult.getSql().equals("(CAST(meteor.age AS INT) < 31)"));
    }

    @Test
    @DisplayName("Given statement of two problematic AND clauses, expect result of both of them at the debug result.")
    void givenStatementOfTwoAndClauses_whenDebuggingFailure_thenExpectTheProblematicStatementsToReturn() {
        // SQL can be run over a temporary view created using DataFrames
        String sqlStatement = "SELECT * FROM meteor WHERE name=\"Ou\" AND (age<31 OR age>50) AND job=\"Officer\"";
        List<DebugResult> debugResults = new ArrayList<>();
        new QueriesInvestigator().investigateQueryExecution(new QueryInvestigation(debugResults,
                dataFrame,
                DebugMode.ON_FAILURE,
                DebuggingAccuracyLevel.FIND_KEY_STATEMENT,
                DebugDepthLevel.LEAF),
                getFilterCondition(sqlStatement));
        Assertions.assertThat(debugResults)
                .hasSize(2)
                .anyMatch(debugResult -> debugResult.getSql().equals("(meteor.job = 'Officer')"))
                .anyMatch(debugResult -> debugResult.getSql().equals("(meteor.name = 'Ou')"));
    }

    // TODO: make these tests one big test with many asserts
    @Test
    void givenStatementOfLikeClause_whenDebuggingFailure_thenExpectTheProblematicStatementsToReturn() {
        String sqlStatement = "SELECT * FROM meteor WHERE name LIKE \"O%\"";
        List<DebugResult> debugResults = new ArrayList<>();
        new QueriesInvestigator().investigateQueryExecution(new QueryInvestigation(debugResults,
                dataFrame,
                DebugMode.ON_FAILURE,
                DebuggingAccuracyLevel.FIND_KEY_STATEMENT,
                DebugDepthLevel.LEAF),
                getFilterCondition(sqlStatement));
        Assertions.assertThat(debugResults)
                .hasSize(1)
                .anyMatch(debugResult -> debugResult.getSql().equals("meteor.name LIKE 'O%'"));
    }

    @Test
    void givenStatementOfLikeClause_whenDebuggingSuccess_thenExpectTheProblematicStatementsToReturn() {
        String sqlStatement = "SELECT * FROM meteor WHERE name LIKE \"G%\"";
        List<DebugResult> debugResults = new ArrayList<>();
        new QueriesInvestigator().investigateQueryExecution(new QueryInvestigation(debugResults,
                dataFrame,
                DebugMode.ON_SUCCESS,
                DebuggingAccuracyLevel.FIND_KEY_STATEMENT,
                DebugDepthLevel.LEAF),
                getFilterCondition(sqlStatement));
        Assertions.assertThat(debugResults)
                .hasSize(1)
                .anyMatch(debugResult -> debugResult.getSql().equals("meteor.name LIKE 'G%'"));
    }

    @Test
    void givenStatementOfGreaterThanClause_whenDebuggingFailure_thenExpectTheProblematicStatementsToReturn() {
        String sqlStatement = "SELECT * FROM meteor WHERE age > 100";
        List<DebugResult> debugResults = new ArrayList<>();
        new QueriesInvestigator().investigateQueryExecution(new QueryInvestigation(debugResults,
                dataFrame,
                DebugMode.ON_FAILURE,
                DebuggingAccuracyLevel.FIND_KEY_STATEMENT,
                DebugDepthLevel.LEAF),
                getFilterCondition(sqlStatement));
        Assertions.assertThat(debugResults)
                .hasSize(1)
                .anyMatch(debugResult -> debugResult.getSql().equals("(CAST(meteor.age AS INT) > 100)"));
    }

    @Test
    void givenStatementOfGreaterThanOrEqualClause_whenDebuggingFailure_thenExpectTheProblematicStatementsToReturn() {
        String sqlStatement = "SELECT * FROM meteor WHERE age >= 100";
        List<DebugResult> debugResults = new ArrayList<>();
        new QueriesInvestigator().investigateQueryExecution(new QueryInvestigation(debugResults,
                dataFrame,
                DebugMode.ON_FAILURE,
                DebuggingAccuracyLevel.FIND_KEY_STATEMENT,
                DebugDepthLevel.LEAF),
                getFilterCondition(sqlStatement));
        Assertions.assertThat(debugResults)
                .hasSize(1)
                .anyMatch(debugResult -> debugResult.getSql().equals("(CAST(meteor.age AS INT) >= 100)"));
    }

    @Test
    void givenStatementOfLessThanClause_whenDebuggingFailure_thenExpectTheProblematicStatementsToReturn() {
        String sqlStatement = "SELECT * FROM meteor WHERE age < 10";
        List<DebugResult> debugResults = new ArrayList<>();
        new QueriesInvestigator().investigateQueryExecution(new QueryInvestigation(debugResults,
                dataFrame,
                DebugMode.ON_FAILURE,
                DebuggingAccuracyLevel.FIND_KEY_STATEMENT,
                DebugDepthLevel.LEAF),
                getFilterCondition(sqlStatement));
        Assertions.assertThat(debugResults)
                .hasSize(1)
                .anyMatch(debugResult -> debugResult.getSql().equals("(CAST(meteor.age AS INT) < 10)"));
    }

    @Test
    void givenStatementOfLessThanOrEqualClause_whenDebuggingFailure_thenExpectTheProblematicStatementsToReturn() {
        String sqlStatement = "SELECT * FROM meteor WHERE age <= 10";
        List<DebugResult> debugResults = new ArrayList<>();
        new QueriesInvestigator().investigateQueryExecution(new QueryInvestigation(debugResults,
                dataFrame,
                DebugMode.ON_FAILURE,
                DebuggingAccuracyLevel.FIND_KEY_STATEMENT,
                DebugDepthLevel.LEAF),
                getFilterCondition(sqlStatement));
        Assertions.assertThat(debugResults)
                .hasSize(1)
                .anyMatch(debugResult -> debugResult.getSql().equals("(CAST(meteor.age AS INT) <= 10)"));
    }
}