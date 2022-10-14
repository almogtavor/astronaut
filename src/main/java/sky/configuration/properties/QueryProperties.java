package sky.configuration.properties;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldNameConstants;
import sky.configuration.debug.DebugDepthLevel;
import sky.configuration.debug.DebuggingAccuracyLevel;

@Builder
@Data
@FieldNameConstants
public class QueryProperties {
    private String queryName;
    /**
     * Represents the name of the queue that the resulted output will get sent to.
     * It is possible that multiples queries will have the same destination queue.
     * Currently, it is not possible to send the same query to multiple queues.
     */
    private String queueName;
    private QueueTechnology queueTechnology;
    private String sqlQuery;
    private DebugDepthLevel debugDepthLevel;
    private DebuggingAccuracyLevel debuggingAccuracyLevel;
    private Boolean debugEnabled;
}
