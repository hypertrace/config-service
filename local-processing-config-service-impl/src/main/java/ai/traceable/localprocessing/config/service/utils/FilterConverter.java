package ai.traceable.localprocessing.config.service.utils;

import ai.traceable.localprocessing.config.service.v1.LogicalOperator;
import ai.traceable.localprocessing.config.service.v1.RelationalOperator;
import ai.traceable.localprocessing.config.service.v1.SpanFilter;
import ai.traceable.localprocessing.config.service.v1.SpanFilterValue;
import ai.traceable.span.processing.config.service.v1.LogicalSpanFilterExpression;
import ai.traceable.span.processing.config.service.v1.RelationalSpanFilterExpression;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FilterConverter {

  private static final String URL_SPAN_ATTRIBUTE_KEY = "http.url";

  public Optional<SpanFilter> convert(
      ai.traceable.span.processing.config.service.v1.SpanFilter filter) {
    SpanFilter.Builder filterBuilder = SpanFilter.newBuilder();
    if (filter.hasLogicalSpanFilter()) {
      ai.traceable.localprocessing.config.service.v1.LogicalSpanFilterExpression
          logicalSpanFilterExpression = convertLogicalFilter(filter.getLogicalSpanFilter());
      if (logicalSpanFilterExpression.getOperandsCount() == 1) {
        return Optional.of(logicalSpanFilterExpression.getOperands(0));
      }
      return Optional.of(
          filterBuilder
              .setLogicalFilter(convertLogicalFilter(filter.getLogicalSpanFilter()))
              .build());
    } else {
      Optional<ai.traceable.localprocessing.config.service.v1.RelationalSpanFilterExpression>
          relationalSpanFilterExpression =
              convertRelationalFilter(filter.getRelationalSpanFilter());
      if (relationalSpanFilterExpression.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(
          filterBuilder.setRelationalFilter(relationalSpanFilterExpression.get()).build());
    }
  }

  private ai.traceable.localprocessing.config.service.v1.LogicalSpanFilterExpression
      convertLogicalFilter(LogicalSpanFilterExpression logicalSpanFilter) {
    return ai.traceable.localprocessing.config.service.v1.LogicalSpanFilterExpression.newBuilder()
        .setOperator(convertLogicalOperator(logicalSpanFilter.getOperator()))
        .addAllOperands(
            logicalSpanFilter.getOperandsList().stream()
                .map(this::convert)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toUnmodifiableList()))
        .build();
  }

  private Optional<ai.traceable.localprocessing.config.service.v1.RelationalSpanFilterExpression>
      convertRelationalFilter(
          ai.traceable.span.processing.config.service.v1.RelationalSpanFilterExpression
              relationalSpanFilter) {
    Optional<String> spanAttributeKey = getSpanAttributeKey(relationalSpanFilter);
    if (spanAttributeKey.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        ai.traceable.localprocessing.config.service.v1.RelationalSpanFilterExpression.newBuilder()
            .setOperator(convertRelationalOperator(relationalSpanFilter.getOperator()))
            .setSpanAttributeKey(spanAttributeKey.get())
            .setRightOperand(
                SpanFilterValue.newBuilder()
                    .setStringValue(relationalSpanFilter.getRightOperand().getStringValue())
                    .build())
            .build());
  }

  private Optional<String> getSpanAttributeKey(
      RelationalSpanFilterExpression relationalSpanFilterExpression) {
    if (relationalSpanFilterExpression.hasSpanAttributeKey()) {
      return Optional.of(relationalSpanFilterExpression.getSpanAttributeKey());
    }
    switch (relationalSpanFilterExpression.getField()) {
      case FIELD_URL:
        return Optional.of(URL_SPAN_ATTRIBUTE_KEY);
      case FIELD_SERVICE_NAME:
      case FIELD_ENVIRONMENT_NAME:
        return Optional.empty();
      default:
        log.error("Unknown span filter field type: {}", relationalSpanFilterExpression.getField());
        return Optional.empty();
    }
  }

  private RelationalOperator convertRelationalOperator(
      ai.traceable.span.processing.config.service.v1.RelationalOperator relationalOperator) {
    switch (relationalOperator) {
      case RELATIONAL_OPERATOR_CONTAINS:
        return RelationalOperator.RELATIONAL_OPERATOR_CONTAINS;
      case RELATIONAL_OPERATOR_STARTS_WITH:
        return RelationalOperator.RELATIONAL_OPERATOR_STARTS_WITH;
      case RELATIONAL_OPERATOR_ENDS_WITH:
        return RelationalOperator.RELATIONAL_OPERATOR_ENDS_WITH;
      case RELATIONAL_OPERATOR_EQUALS:
        return RelationalOperator.RELATIONAL_OPERATOR_EQUALS;
      case RELATIONAL_OPERATOR_NOT_EQUALS:
        return RelationalOperator.RELATIONAL_OPERATOR_NOT_EQUALS;
      case RELATIONAL_OPERATOR_REGEX_MATCH:
        return RelationalOperator.RELATIONAL_OPERATOR_REGEX_MATCH;
      default: // TODO: do we want to throw or log error considering these would be used by agent as
        // well
        throw new UnsupportedOperationException(
            "unknown relational operator type: " + relationalOperator);
    }
  }

  private LogicalOperator convertLogicalOperator(
      ai.traceable.span.processing.config.service.v1.LogicalOperator logicalOperator) {
    switch (logicalOperator) {
      case LOGICAL_OPERATOR_AND:
        return LogicalOperator.LOGICAL_OPERATOR_AND;
      case LOGICAL_OPERATOR_OR:
        return LogicalOperator.LOGICAL_OPERATOR_OR;
      default:
        throw new UnsupportedOperationException(
            "unknown logical operator type: " + logicalOperator);
    }
  }
}
