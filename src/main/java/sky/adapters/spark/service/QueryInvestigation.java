package sky.adapters.spark.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Expression;
import sky.configuration.properties.DebugDepthLevel;
import sky.configuration.properties.DebugMode;
import sky.configuration.properties.DebuggingAccuracyLevel;
import sky.model.DebugResult;

import java.util.List;

public record QueryInvestigation(List<DebugResult> debugResults, Dataset<Row> rowDataframe,
                                 DebugMode debugMode, DebuggingAccuracyLevel debuggingAccuracyLevel,
                                 DebugDepthLevel debugDepthLevel) {}
