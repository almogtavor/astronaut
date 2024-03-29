package sky.engines.spark.service;

import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.extension.ExtendWith;
import sky.configuration.debug.DebugDepthLevel;
import sky.configuration.debug.DebugMode;
import sky.configuration.debug.DebuggingAccuracyLevel;
import sky.model.DebugResult;

@ExtendWith(SparkExtension.class)
class CepExecutorSimpleSchemaTest {
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
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("George","30","Developer"));
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
            "SELECT * FROM meteor WHERE name=\"Ou\" AND (age<31 OR age>50), (meteor.name = 'Ou')",
            "SELECT * FROM meteor WHERE name IS NULL, (meteor.name IS NULL)",
            "SELECT * FROM meteor WHERE name !=\"George\", (NOT (meteor.name = 'George'))",
            "SELECT * FROM meteor WHERE name LIKE \"O%\", meteor.name LIKE 'O%'",
            "SELECT * FROM meteor WHERE age > 100, (CAST(meteor.age AS INT) > 100)",
            "SELECT * FROM meteor WHERE age >= 100, (CAST(meteor.age AS INT) >= 100)",
            "SELECT * FROM meteor WHERE age < 10, (CAST(meteor.age AS INT) < 10)",
            "SELECT * FROM meteor WHERE age <= 10, (CAST(meteor.age AS INT) <= 10)"
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
            "SELECT * FROM meteor WHERE name IS NOT NULL, (meteor.name IS NOT NULL)",
            "SELECT * FROM meteor WHERE name LIKE \"G%\", meteor.name LIKE 'G%'",
            "SELECT * FROM meteor WHERE age > 10, (CAST(meteor.age AS INT) > 10)",
            "SELECT * FROM meteor WHERE age >= 10, (CAST(meteor.age AS INT) >= 10)",
            "SELECT * FROM meteor WHERE age < 100, (CAST(meteor.age AS INT) < 100)",
            "SELECT * FROM meteor WHERE age <= 100, (CAST(meteor.age AS INT) <= 100)"
    })
    void givenStatementOfOneProblematicClause_whenDebuggingSuccess_thenExpectTheProblematicAndStatementToReturn(String sqlStatement, String problematicQuery) {
        List<DebugResult> debugResults = new ArrayList<>();
        new QueriesInvestigator().investigateQueryExecution(new QueryInvestigation(debugResults,
                dataFrame,
                DebugMode.ON_SUCCESS,
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

    @Test
    @DisplayName("Given statement of two failed OR leaf statements, expect result of both of them at the debug result.")
    void givenStatementOfOrClauseOfTwoFailures_whenDebuggingFailure_thenExpectTheProblematicStatementsToReturn() {
        String sqlStatement = "SELECT * FROM meteor WHERE name=\"Ou\" OR job=\"Officer\"";
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

    @Test
    @DisplayName("Given statement of two OR leaf statements (one success one failure), expect the non problematic statement to return as a debug result.")
    void givenStatementOfOrClause_whenDebuggingFailureWithFindAllUnexpectedStatements_thenExpectTheNonProblematicStatementToReturn() {
        String sqlStatement = "SELECT * FROM meteor WHERE name=\"George\" OR job=\"Officer\"";
        List<DebugResult> debugResults = new ArrayList<>();
        new QueriesInvestigator().investigateQueryExecution(new QueryInvestigation(debugResults,
                dataFrame,
                DebugMode.ON_FAILURE,
                DebuggingAccuracyLevel.FIND_ALL_UNEXPECTED,
                DebugDepthLevel.LEAF),
                getFilterCondition(sqlStatement));
        Assertions.assertThat(debugResults)
                .hasSize(1)
                .anyMatch(debugResult -> debugResult.getSql().equals("(meteor.job = 'Officer')"));
    }
}