package ai.traceable.config.utils;

import ai.traceable.span.processing.config.service.v1.Field;
import ai.traceable.span.processing.config.service.v1.ListValue;
import ai.traceable.span.processing.config.service.v1.LogicalOperator;
import ai.traceable.span.processing.config.service.v1.RelationalSpanFilterExpression;
import ai.traceable.span.processing.config.service.v1.SpanFilter;
import com.google.re2j.Pattern;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SpanFilterMatcher {

  public boolean hasOnlyEnvironmentAndServiceNameFilters(final SpanFilter spanFilter) {

    if (spanFilter.hasRelationalSpanFilter()) {
      final RelationalSpanFilterExpression relationalSpanFilter =
          spanFilter.getRelationalSpanFilter();
      return hasEnvironmentFilter(relationalSpanFilter)
          || hasServiceNameFilter(relationalSpanFilter);
    } else if (spanFilter.hasLogicalSpanFilter()) {
      /**
       * Since environment and service name filters are expected at the first level only, any other
       * nested logical filter would mean presence of a span filter that contains filters other than
       * service and environment ones
       */
      return spanFilter.getLogicalSpanFilter().getOperandsList().stream()
              .filter(SpanFilter::hasRelationalSpanFilter)
              .allMatch(this::hasOnlyEnvironmentAndServiceNameFilters)
          && !hasLogicalSpanFilter(spanFilter.getLogicalSpanFilter().getOperandsList());
    }

    // no presence of span filters implies filter is eligible for all environments and services
    return true;
  }

  public boolean hasEnvironmentFilter(
      RelationalSpanFilterExpression relationalSpanFilterExpression) {
    return relationalSpanFilterExpression.hasField()
        && relationalSpanFilterExpression.getField().equals(Field.FIELD_ENVIRONMENT_NAME);
  }

  public boolean hasServiceNameFilter(
      RelationalSpanFilterExpression relationalSpanFilterExpression) {
    return relationalSpanFilterExpression.hasField()
        && relationalSpanFilterExpression.getField().equals(Field.FIELD_SERVICE_NAME);
  }

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
        List<SpanFilter> envFilters =
            spanFilter.getLogicalSpanFilter().getOperandsList().stream()
                .filter(SpanFilter::hasRelationalSpanFilter)
                .filter(filter -> hasEnvironmentFilter(filter.getRelationalSpanFilter()))
                .collect(Collectors.toList());
        if (envFilters.isEmpty()) {
          return true;
        }
        return envFilters.stream()
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
        List<SpanFilter> svcFilters =
            spanFilter.getLogicalSpanFilter().getOperandsList().stream()
                .filter(SpanFilter::hasRelationalSpanFilter)
                .filter(filter -> hasServiceNameFilter(filter.getRelationalSpanFilter()))
                .collect(Collectors.toList());
        if (svcFilters.isEmpty()) {
          return true;
        }
        return svcFilters.stream()
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

  public boolean matches(
      String lhs,
      ai.traceable.span.processing.config.service.v1.SpanFilterValue rhs,
      ai.traceable.span.processing.config.service.v1.RelationalOperator relationalOperator) {
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

  private boolean matches(
      String lhs,
      String rhs,
      ai.traceable.span.processing.config.service.v1.RelationalOperator relationalOperator) {
    try {
      switch (relationalOperator) {
        case RELATIONAL_OPERATOR_CONTAINS:
          return lhs.contains(rhs);
        case RELATIONAL_OPERATOR_NOT_CONTAINS:
          return !lhs.contains(rhs);
        case RELATIONAL_OPERATOR_EQUALS:
          return lhs.equals(rhs);
        case RELATIONAL_OPERATOR_NOT_EQUALS:
          return !lhs.equals(rhs);
        case RELATIONAL_OPERATOR_STARTS_WITH:
          return lhs.startsWith(rhs);
        case RELATIONAL_OPERATOR_ENDS_WITH:
          return lhs.endsWith(rhs);
        case RELATIONAL_OPERATOR_REGEX_MATCH:
          return Pattern.compile(rhs).matcher(lhs).find();
        default:
          throw new IllegalStateException(
              "Unsupported relational operator for string value rhs: {}" + relationalOperator);
      }
    } catch (Exception e) {
      log.error(
          "Unable to match lhs: {} with rhs: {} for operator: {}", lhs, rhs, relationalOperator);
      return false;
    }
  }

  private boolean matches(
      String lhs,
      ListValue rhs,
      ai.traceable.span.processing.config.service.v1.RelationalOperator relationalOperator) {
    switch (relationalOperator) {
      case RELATIONAL_OPERATOR_IN:
        return rhs.getValuesList().stream()
            .map(ai.traceable.span.processing.config.service.v1.SpanFilterValue::getStringValue)
            .collect(Collectors.toUnmodifiableList())
            .contains(lhs);
      default:
        log.error("Unsupported relational operator for list value rhs:{}", relationalOperator);
        return false;
    }
  }

  private boolean hasLogicalSpanFilter(final List<SpanFilter> spanFilters) {
    return spanFilters.stream().anyMatch(SpanFilter::hasLogicalSpanFilter);
  }
}
