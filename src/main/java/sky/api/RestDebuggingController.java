package sky.api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import sky.engines.spark.service.QueriesInvestigator;
import sky.engines.spark.service.QueryInvestigation;
import sky.configuration.debug.DebugDepthLevel;
import sky.configuration.debug.DebugMode;
import sky.configuration.debug.DebuggingAccuracyLevel;
import sky.model.DebugResult;

import java.util.ArrayList;
import java.util.List;

@RestController
@Controller
public class RestDebuggingController {
    private SparkSession spark;
    private QueriesInvestigator queriesInvestigator;

    @GetMapping("{id}")
    public ResponseEntity<Mono<List<DebugResult>>> debugId(@PathVariable("id") String id, String sqlStatement,
                                                           DebugMode debugMode, DebuggingAccuracyLevel debuggingAccuracyLevel,
                                                           DebugDepthLevel debugDepthLevel) {
        // TODO: query the id and extract a json from it
        // TODO: turn this json into a dataframe
        List<DebugResult> debugResults = new ArrayList<>();
        StructType schema = DataTypes.createStructType(List.of(
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.StringType, true),
                DataTypes.createStructField("job", DataTypes.StringType, true)));
        List<Row> nums = new ArrayList<>();
        nums.add(RowFactory.create("George","30","Developer"));
        Dataset<Row> dataFrame = spark.createDataFrame(nums, schema);
        dataFrame.createOrReplaceTempView("meteor");

        queriesInvestigator.investigateQueryExecution(new QueryInvestigation(debugResults,
                        dataFrame,
                        debugMode,
                        debuggingAccuracyLevel,
                        debugDepthLevel),
                getFilterCondition(dataFrame, sqlStatement));
        return ResponseEntity.ok().body(Mono.just(debugResults));
    }

    private Expression getFilterCondition(Dataset<Row> dataFrame, String sqlStatement) {
        QueryExecution queryExecution = dataFrame.sqlContext().sql(sqlStatement).queryExecution();
        LogicalPlan analyzed = queryExecution.analyzed();
        Filter filter = (Filter)((Project) analyzed).child();
        return filter.condition();
    }
}
