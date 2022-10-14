package sky.engines.spark.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.execution.QueryExecution;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import sky.configuration.debug.DebugDepthLevel;
import sky.configuration.debug.DebuggingAccuracyLevel;
import sky.configuration.properties.*;
import sky.model.DebugResult;

import java.util.ArrayList;
import java.util.List;

@Service
@AllArgsConstructor
@Slf4j
public class SparkCepExecutor {
    private final SparkSession spark;
    private final AstronautProperties astronautProperties;
    @Qualifier("queues")
    private final List<QueryProperties> queriesProperties;
    private final QueriesInvestigator queriesInvestigator;
    private final SparkQueuesPublisher sparkQueuesPublisher;

    public void executeSqlStatements(Dataset<Row> df, SinkType sinkType) {
        df.createOrReplaceTempView("meteor");

        for (var queryProperties : queriesProperties) {
            Dataset<Row> results = spark.sql(queryProperties.getSqlQuery());
            results.show();

            executeQueryDebuggingProcess(queryProperties, results);
            results = sparkQueuesPublisher.prepareResultsForPublishing(queryProperties, results);
            sparkQueuesPublisher.publishMessagesToQueues(sinkType, queryProperties, results);
        }
    }

    private void executeQueryDebuggingProcess(QueryProperties queryProperties, Dataset<Row> results) {
        if (astronautProperties.isDebugEnabled() && queryProperties.getDebugEnabled()) {
            QueryExecution queryExecution = results.queryExecution();
            if (queryExecution.analyzed() instanceof Project project) {
                if (project.child() instanceof Filter filter) {
                    List<DebugResult> debugResults = new ArrayList<>();
                    log.info("The SQL statement is: " + queryProperties.getSqlQuery());
                    queriesInvestigator.investigateQueryExecution(new QueryInvestigation(debugResults,
                                    results,
                                    astronautProperties.getDebugMode(),
                                    DebuggingAccuracyLevel.FIND_KEY_STATEMENT,
                                    DebugDepthLevel.LEAF),
                            filter.condition());
                    log.info("debugResults are: " + debugResults);
                } else {
                    log.error("Unexpected type for project.child()");
                }
            } else {
                log.error("CepExecutor.executeSqlStatements");
            }
        }
    }

}
