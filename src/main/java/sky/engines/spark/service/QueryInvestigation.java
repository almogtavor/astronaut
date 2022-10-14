package sky.engines.spark.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import sky.configuration.debug.DebugDepthLevel;
import sky.configuration.debug.DebugMode;
import sky.configuration.debug.DebuggingAccuracyLevel;
import sky.model.DebugResult;

import java.util.List;

public record QueryInvestigation(List<DebugResult> debugResults, Dataset<Row> rowDataframe,
                                 DebugMode debugMode, DebuggingAccuracyLevel debuggingAccuracyLevel,
                                 DebugDepthLevel debugDepthLevel) {}
