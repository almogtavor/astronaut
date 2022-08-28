package sky.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class DebugResult {
    private String sql;
    private boolean isSuccess;
    private String operator;
    private String rowValuesJson;
    private boolean isLeafStatement;

    @Override
    public String toString() {
        return "The reason for failure was that the sql: '" + sql + '\'' +
               ", returned success status of: " + isSuccess +
               ", operator='" + operator + '\'' +
               ", rowValuesJson='" + rowValuesJson + '\'' +
               ", isLeafStatement='" + isLeafStatement + '\'' +
               '}';
    }
}
