package org.hypertrace.config.service.store;

import io.grpc.Status;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.config.service.ConfigServiceUtils;
import org.hypertrace.config.service.v1.Filter;
import org.hypertrace.config.service.v1.LogicalFilter;
import org.hypertrace.config.service.v1.RelationalFilter;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;

import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EXISTS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_EXISTS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_IN;

public class FilterExpressionBuilder {
  private static final String DEFAULT_CONSTANT_EXPRESSION_VALUE = "";

  public FilterTypeExpression buildFilterTypeExpression(Filter filter) {
    switch (filter.getTypeCase()) {
      case LOGICAL_FILTER:
        return buildLogicalExpression(filter.getLogicalFilter());
      case RELATIONAL_FILTER:
        return buildRelationalExpression(filter.getRelationalFilter());
      case TYPE_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT.withDescription("Filter type unset").asRuntimeException();
    }
  }

  private FilterTypeExpression buildLogicalExpression(LogicalFilter logicalFilter) {
    List<FilterTypeExpression> childExpressions =
        logicalFilter.getOperandsList().stream()
            .map(this::buildFilterTypeExpression)
            .collect(Collectors.toUnmodifiableList());

    LogicalOperator operator;
    switch (logicalFilter.getOperator()) {
      case LOGICAL_OPERATOR_AND:
        operator = LogicalOperator.AND;
        break;
      case LOGICAL_OPERATOR_OR:
        operator = LogicalOperator.OR;
        break;
      case LOGICAL_OPERATOR_UNSPECIFIED:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unknown logical operator while building expression")
            .asRuntimeException();
    }
    return LogicalExpression.builder().operator(operator).operands(childExpressions).build();
  }

  private FilterTypeExpression buildRelationalExpression(RelationalFilter relationalFilter) {
    RelationalOperator operator;
    switch (relationalFilter.getOperator()) {
      case RELATIONAL_OPERATOR_EQ:
        operator = EQ;
        break;
      case RELATIONAL_OPERATOR_NEQ:
        operator = NEQ;
        break;
      case RELATIONAL_OPERATOR_IN:
        operator = IN;
        break;
      case RELATIONAL_OPERATOR_NOT_IN:
        operator = NOT_IN;
        break;
      case RELATIONAL_OPERATOR_LT:
        operator = LT;
        break;
      case RELATIONAL_OPERATOR_GT:
        operator = GT;
        break;
      case RELATIONAL_OPERATOR_LTE:
        operator = LTE;
        break;
      case RELATIONAL_OPERATOR_GTE:
        operator = GTE;
        break;
      case RELATIONAL_OPERATOR_EXISTS:
        operator = EXISTS;
        break;
      case RELATIONAL_OPERATOR_NOT_EXISTS:
        operator = NOT_EXISTS;
        break;
      case UNRECOGNIZED:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unknown relational operator while building expression")
            .asRuntimeException();
    }

    if(EXISTS.equals(operator) || NOT_EXISTS.equals(operator)) {
      return RelationalExpression.of(
              IdentifierExpression.of(
                      ConfigServiceUtils.buildConfigFieldPath(relationalFilter.getConfigJsonPath())),
              operator,
              ConstantExpression.of(DEFAULT_CONSTANT_EXPRESSION_VALUE));
    }
    return RelationalExpression.of(
        IdentifierExpression.of(
            ConfigServiceUtils.buildConfigFieldPath(relationalFilter.getConfigJsonPath())),
        operator,
        ConstantExpressionConverter.fromProtoValue(relationalFilter.getValue()));
  }
}
