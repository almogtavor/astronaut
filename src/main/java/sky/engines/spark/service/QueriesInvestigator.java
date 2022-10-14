package sky.engines.spark.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.*;
import org.springframework.stereotype.Service;
import sky.model.DebugResult;

@Service
@Slf4j
public class QueriesInvestigator {
    /**
     * The expressions that are currently supported for debugging are the expressions of org.apache.spark.sql.catalyst.expressions.
     */
    // TODO: change the "should search for"
    // TODO: pass an object instead of million parameters
    public void investigateQueryExecution(QueryInvestigation queryInvestigation, Expression expression) {
        switch (expression) {
            case EqualTo equalTo -> handleBinaryExpression(queryInvestigation, equalTo);
            case Or or -> handleBinaryOperator(queryInvestigation, or);
            case And and -> handleBinaryOperator(queryInvestigation, and);
            case In in -> handleExpression(queryInvestigation, in); //TODO
            case InSet inSet -> handleExpression(queryInvestigation, inSet); //TODO
            case Not not -> handleExpression(queryInvestigation, not);
            case EqualNullSafe equalNullSafe -> handleExpression(queryInvestigation, equalNullSafe); //TODO
            case LessThan lessThan -> handleBinaryExpression(queryInvestigation, lessThan);
            case LessThanOrEqual lessThanOrEqual -> handleBinaryExpression(queryInvestigation, lessThanOrEqual);
            case GreaterThan greaterThan -> handleBinaryExpression(queryInvestigation, greaterThan);
            case GreaterThanOrEqual greaterThanOrEqual -> handleBinaryExpression(queryInvestigation, greaterThanOrEqual);
            case IsNull isNull -> handleExpression(queryInvestigation, isNull);
            case IsNotNull isNotNull -> handleExpression(queryInvestigation, isNotNull);
            case Like like -> handleBinaryExpression(queryInvestigation, like);
            case If anIf -> handleExpression(queryInvestigation, anIf); //TODO
            case CaseWhen caseWhen -> handleExpression(queryInvestigation, caseWhen); //TODO
            default -> System.out.println("unexpected expression: " + expression);
        }
    }

    /**
     * Note that DebugResultLevel.HIGHEST_PROBLEMATIC_NODE combined with DebuggingAccuracyLevel.FIND_ALL_UNEXPECTED will result a wierd behaviour.
     * TODO: add depth
     */
    private void handleBinaryOperator(QueryInvestigation queryInvestigation,
                                      BinaryOperator binaryOperator) {
        switch (queryInvestigation.debuggingAccuracyLevel()) {
            case FIND_KEY_STATEMENT -> {
                ExpressionHandler expressionHandler = ExpressionHandler.handle(binaryOperator);
                switch (queryInvestigation.debugMode()) {
                    case ON_FAILURE -> {
                        if (isQuerySucceeded(queryInvestigation.rowDataframe(), expressionHandler.getStatement())) {
                            log.debug("The reason for failure wasn't: " + binaryOperator.sql());
                        } else {
                            investigateQueryExecution(queryInvestigation, binaryOperator.left());
                            investigateQueryExecution(queryInvestigation, binaryOperator.right());
                        }
                    }
                    case ON_SUCCESS -> {
                        if (isQuerySucceeded(queryInvestigation.rowDataframe(), expressionHandler.getStatement())) {
                            investigateQueryExecution(queryInvestigation, binaryOperator.left());
                            investigateQueryExecution(queryInvestigation, binaryOperator.right());
                        } else {
                            log.debug("The reason for success wasn't: " + binaryOperator.sql());
                        }
                    }
                    case NEVER -> {}
                    case default -> log.error("unexpected debug mode: " + queryInvestigation.debugMode());
                }
            }
            case FIND_ALL_UNEXPECTED -> {
                investigateQueryExecution(queryInvestigation, binaryOperator.left());
                investigateQueryExecution(queryInvestigation, binaryOperator.right());
            }
        }
    }

    /**
     * {@link LeafExpression} is an object that represents an object without any child expressions.
     * For example: {@link Literal}.
     * An {@link UnaryExpression} is an object that represents an expression with one input and one output, e.g. some operator.
     * For example: {@link Cast}.
     */
    private void handleBinaryExpression(QueryInvestigation queryInvestigation,
                                        BinaryExpression binaryExpression) {
        if ((binaryExpression.left() instanceof LeafExpression || binaryExpression.left() instanceof UnaryExpression)
            && (binaryExpression.right() instanceof LeafExpression || binaryExpression.right() instanceof UnaryExpression)) {
            handleExpression(queryInvestigation, binaryExpression);
        } else {
            handleChildExpressions(queryInvestigation, binaryExpression);
        }
    }

    private void handleExpression(QueryInvestigation queryInvestigation, Expression expression) {
        ExpressionHandler expressionHandler = ExpressionHandler.handle(expression);
        switch (queryInvestigation.debugMode()) {
            case ON_FAILURE -> {
                if (isQuerySucceeded(queryInvestigation.rowDataframe(), expressionHandler.getStatement())) {
                    log.debug("The reason for failure wasn't: " + expression.sql());
                } else {
                    Row row = queryInvestigation.rowDataframe().collectAsList().get(0);
                    queryInvestigation.debugResults().add(new DebugResult(expression.sql(), false, expression.nodeName(), row.json(), expressionHandler.isLeafStatement()));
                }
            }
            case ON_SUCCESS -> {
                if (isQuerySucceeded(queryInvestigation.rowDataframe(), expressionHandler.getStatement())) {
                    Row row = queryInvestigation.rowDataframe().collectAsList().get(0);
                    queryInvestigation.debugResults().add(new DebugResult(expression.sql(), true, expression.nodeName(), row.json(), expressionHandler.isLeafStatement()));
                } else {
                    log.debug("The reason for success wasn't: " + expression.sql());
                }
            }
            case NEVER -> {}
            case default -> log.error("unexpected debug mode: " + queryInvestigation.debugMode());
        }
    }

    private void handleChildExpressions(QueryInvestigation queryInvestigation,
                                        BinaryExpression binaryOperator) {
        if (!(binaryOperator.left() instanceof LeafExpression
              || binaryOperator.left() instanceof UnaryMathExpression
              || binaryOperator.left() instanceof UnaryExpression)) {
            investigateQueryExecution(queryInvestigation, binaryOperator.left());
        }
        if (!(binaryOperator.left() instanceof LeafExpression
              || binaryOperator.left() instanceof UnaryMathExpression
              || binaryOperator.left() instanceof UnaryExpression)) {
            investigateQueryExecution(queryInvestigation, binaryOperator.right());
        }
    }

    private boolean isQuerySucceeded(Dataset<Row> rowDataset, String sqlStatement) {
        return switch (Math.toIntExact(rowDataset.sqlContext().sql(sqlStatement).count())) {
            case 0 -> false;
            case 1 -> true;
            default -> throw new IllegalStateException("Unexpected returned rows count (must be a bug): " + Math.toIntExact(rowDataset.sqlContext().sql(sqlStatement).count()));
        };
    }
}
