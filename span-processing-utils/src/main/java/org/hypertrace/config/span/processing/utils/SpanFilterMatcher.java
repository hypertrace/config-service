package org.hypertrace.config.span.processing.utils;

import com.google.re2j.Pattern;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.span.processing.config.service.v1.Field;
import org.hypertrace.span.processing.config.service.v1.ListValue;
import org.hypertrace.span.processing.config.service.v1.LogicalOperator;
import org.hypertrace.span.processing.config.service.v1.RelationalOperator;
import org.hypertrace.span.processing.config.service.v1.RelationalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.SpanFilter;
import org.hypertrace.span.processing.config.service.v1.SpanFilterValue;

@Slf4j
public class SpanFilterMatcher {

  public boolean matchesEnvironment(SpanFilter spanFilter, Optional<String> environment) {
    if (spanFilter.hasRelationalSpanFilter()) {
      return matchesEnvironment(spanFilter.getRelationalSpanFilter(), environment);
    } else {
      if (spanFilter
          .getLogicalSpanFilter()
          .getOperator()
          .equals(LogicalOperator.LOGICAL_OPERATOR_AND)) {
        return spanFilter.getLogicalSpanFilter().getOperandsList().stream()
            .filter(SpanFilter::hasRelationalSpanFilter)
            .allMatch(filter -> matchesEnvironment(filter.getRelationalSpanFilter(), environment));
      } else {
        if (spanFilter.getLogicalSpanFilter().getOperandsCount() == 0) {
          return true;
        }
        return spanFilter.getLogicalSpanFilter().getOperandsList().stream()
            .filter(SpanFilter::hasRelationalSpanFilter)
            .anyMatch(filter -> matchesEnvironment(filter.getRelationalSpanFilter(), environment));
      }
    }
  }

  public boolean matchesServiceName(SpanFilter spanFilter, String serviceName) {
    if (spanFilter.hasRelationalSpanFilter()) {
      return matchesServiceName(spanFilter.getRelationalSpanFilter(), serviceName);
    } else {
      if (spanFilter
          .getLogicalSpanFilter()
          .getOperator()
          .equals(LogicalOperator.LOGICAL_OPERATOR_AND)) {
        return spanFilter.getLogicalSpanFilter().getOperandsList().stream()
            .filter(SpanFilter::hasRelationalSpanFilter)
            .allMatch(filter -> matchesServiceName(filter.getRelationalSpanFilter(), serviceName));
      } else {
        if (spanFilter.getLogicalSpanFilter().getOperandsCount() == 0) {
          return true;
        }
        return spanFilter.getLogicalSpanFilter().getOperandsList().stream()
            .filter(SpanFilter::hasRelationalSpanFilter)
            .anyMatch(filter -> matchesServiceName(filter.getRelationalSpanFilter(), serviceName));
      }
    }
  }

  private boolean matchesEnvironment(
      RelationalSpanFilterExpression relationalSpanFilterExpression, Optional<String> environment) {
    if (environment.isEmpty()) {
      return true;
    }
    if (relationalSpanFilterExpression.hasField()
        && relationalSpanFilterExpression.getField().equals(Field.FIELD_ENVIRONMENT_NAME)) {
      return matches(
          environment.get(),
          relationalSpanFilterExpression.getRightOperand(),
          relationalSpanFilterExpression.getOperator());
    }
    return true;
  }

  private boolean matchesServiceName(
      RelationalSpanFilterExpression relationalSpanFilterExpression, String serviceName) {
    if (relationalSpanFilterExpression.hasField()
        && relationalSpanFilterExpression.getField().equals(Field.FIELD_SERVICE_NAME)) {
      return matches(
          serviceName,
          relationalSpanFilterExpression.getRightOperand(),
          relationalSpanFilterExpression.getOperator());
    }
    return true;
  }

  public boolean matches(String lhs, SpanFilterValue rhs, RelationalOperator relationalOperator) {
    switch (rhs.getValueCase()) {
      case STRING_VALUE:
        return matches(lhs, rhs.getStringValue(), relationalOperator);
      case LIST_VALUE:
        return matches(lhs, rhs.getListValue(), relationalOperator);
      default:
        log.error("Unknown span filter value type:{}", rhs);
        return false;
    }
  }

  private boolean matches(String lhs, String rhs, RelationalOperator relationalOperator) {
    switch (relationalOperator) {
      case RELATIONAL_OPERATOR_CONTAINS:
        return lhs.contains(rhs);
      case RELATIONAL_OPERATOR_EQUALS:
        return lhs.equals(rhs);
      case RELATIONAL_OPERATOR_NOT_EQUALS:
        return !lhs.equals(rhs);
      case RELATIONAL_OPERATOR_STARTS_WITH:
        return lhs.startsWith(rhs);
      case RELATIONAL_OPERATOR_ENDS_WITH:
        return lhs.endsWith(rhs);
      case RELATIONAL_OPERATOR_REGEX_MATCH:
        try {
          return Pattern.compile(rhs).matcher(lhs).find();
        } catch (Exception e) {
          log.error("Invalid regex: {} passed to match: {}", rhs, e);
          log.debug("Invalid regex passed to match. Hence returning false. lhs: {} and rhs: {}", lhs, rhs);
          return false;
        }
      default:
        log.error("Unsupported relational operator for string value rhs:{}", relationalOperator);
        return false;
    }
  }

  private boolean matches(String lhs, ListValue rhs, RelationalOperator relationalOperator) {
    switch (relationalOperator) {
      case RELATIONAL_OPERATOR_IN:
        return rhs.getValuesList().stream()
            .map(SpanFilterValue::getStringValue)
            .collect(Collectors.toUnmodifiableList())
            .contains(lhs);
      default:
        log.error("Unsupported relational operator for list value rhs:{}", relationalOperator);
        return false;
    }
  }
}
