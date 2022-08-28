package sky.configuration.properties;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class QueueProperties {
    private String sqlStatement;
//    private DebugQuery debugQuery;
    private DebugDepthLevel debugResultLevel;
    private DebuggingAccuracyLevel debuggingAccuracyLevel;

//    @AllArgsConstructor
//    @Data
//    private static class DebugQuery {
//        /**
//         * The statement level to result from the debugging process.
//         */
//        private DebugDepthLevel debugResultLevel;
//        private DebuggingAccuracyLevel debuggingAccuracyLevel;
//    }
}
