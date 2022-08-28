package sky.adapters.spark.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.catalyst.expressions.Expression;
import scala.Option;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class ExpressionHandler {
    private String select;
    private String statement;
    private boolean isLeafStatement;

    public static ExpressionHandler handle(Expression expression) {
        Option<String> sqlText = expression.origin().sqlText();
        String select = sqlText.get().split("WHERE")[0];
        String statement = "%s WHERE %s".formatted(select, expression.sql());
        // TODO: consider removing this:
        boolean isLeafStatement = false; // for initialization only
        return new ExpressionHandler(select, statement, isLeafStatement);
    }
}
