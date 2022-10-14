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
        return "The debug result was that the sql: '" + sql + '\'' +
               ", returned success status of: " + (isSuccess ? "success" : "failure") +
               ", operator='" + operator + '\'' +
               ", rowValuesJson='" + rowValuesJson + '\'' +
               ", isLeafStatement='" + isLeafStatement + '\'' +
               '}';
    }
}
