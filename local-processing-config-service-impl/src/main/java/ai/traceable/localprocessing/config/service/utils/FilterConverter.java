package ai.traceable.localprocessing.config.service.utils;

import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanAttributeConstants.URL_PATH_SPAN_ATTRIBUTE_KEYS;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanAttributeConstants.URL_SPAN_ATTRIBUTE_KEYS;

import ai.traceable.localprocessing.config.service.v1.LogicalOperator;
import ai.traceable.localprocessing.config.service.v1.RelationalOperator;
import ai.traceable.localprocessing.config.service.v1.SpanFilter;
import ai.traceable.localprocessing.config.service.v1.SpanFilterValue;
import ai.traceable.span.processing.config.service.v1.LogicalSpanFilterExpression;
import ai.traceable.span.processing.config.service.v1.RelationalSpanFilterExpression;
import com.google.common.collect.Iterables;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FilterConverter {

  public Optional<SpanFilter> convert(
      ai.traceable.span.processing.config.service.v1.SpanFilter filter) {
    switch (filter.getSpanFilterExpressionCase()) {
      case LOGICAL_SPAN_FILTER:
        return this.convertLogicalFilter(filter.getLogicalSpanFilter());
      case RELATIONAL_SPAN_FILTER:
        return this.convertRelationalFilter(filter.getRelationalSpanFilter());
      case SPANFILTEREXPRESSION_NOT_SET:
        return Optional.empty();
      default:
        log.warn("Unsupported filter case: {}", filter);
        return Optional.empty();
    }
  }

  private Optional<SpanFilter> convertLogicalFilter(LogicalSpanFilterExpression logicalSpanFilter) {
    List<SpanFilter> children =
        logicalSpanFilter.getOperandsList().stream()
            .map(this::convert)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toUnmodifiableList());

    return combineFiltersWithOperator(
        children, convertLogicalOperator(logicalSpanFilter.getOperator()));
  }

  private Optional<SpanFilter> convertRelationalFilter(
      RelationalSpanFilterExpression relationalSpanFilter) {
    List<SpanFilter> individualAttributeFilters =
        getSpanAttributeKeys(relationalSpanFilter).stream()
            .map(
                key ->
                    buildRelationalFilter(
                        key,
                        relationalSpanFilter.getOperator(),
                        relationalSpanFilter.getRightOperand()))
            .collect(Collectors.toUnmodifiableList());

    return combineFiltersWithOperator(
        individualAttributeFilters, LogicalOperator.LOGICAL_OPERATOR_OR);
  }

  private Optional<SpanFilter> combineFiltersWithOperator(
      List<SpanFilter> filters, LogicalOperator operator) {
    if (filters.isEmpty()) {
      return Optional.empty();
    }
    if (filters.size() == 1) {
      return Optional.of(Iterables.getOnlyElement(filters));
    }
    return Optional.of(
        SpanFilter.newBuilder()
            .setLogicalFilter(
                ai.traceable.localprocessing.config.service.v1.LogicalSpanFilterExpression
                    .newBuilder()
                    .setOperator(operator)
                    .addAllOperands(filters))
            .build());
  }

  private SpanFilter buildRelationalFilter(
      String key,
      ai.traceable.span.processing.config.service.v1.RelationalOperator operator,
      ai.traceable.span.processing.config.service.v1.SpanFilterValue filterValue) {
    return SpanFilter.newBuilder()
        .setRelationalFilter(
            ai.traceable.localprocessing.config.service.v1.RelationalSpanFilterExpression
                .newBuilder()
                .setSpanAttributeKey(key)
                .setOperator(convertRelationalOperator(operator))
                .setRightOperand(
                    SpanFilterValue.newBuilder().setStringValue(filterValue.getStringValue())))
        .build();
  }

  private List<String> getSpanAttributeKeys(
      RelationalSpanFilterExpression relationalSpanFilterExpression) {
    if (relationalSpanFilterExpression.hasSpanAttributeKey()) {
      return List.of(relationalSpanFilterExpression.getSpanAttributeKey());
    }
    switch (relationalSpanFilterExpression.getField()) {
      case FIELD_URL:
        return URL_SPAN_ATTRIBUTE_KEYS;
      case FIELD_URL_PATH:
        return URL_PATH_SPAN_ATTRIBUTE_KEYS;
      case FIELD_SERVICE_NAME:
      case FIELD_ENVIRONMENT_NAME:
        return Collections.emptyList();
      default:
        log.error("Unknown span filter field type: {}", relationalSpanFilterExpression.getField());
        return Collections.emptyList();
    }
  }

  private RelationalOperator convertRelationalOperator(
      ai.traceable.span.processing.config.service.v1.RelationalOperator relationalOperator) {
    switch (relationalOperator) {
      case RELATIONAL_OPERATOR_CONTAINS:
        return RelationalOperator.RELATIONAL_OPERATOR_CONTAINS;
      case RELATIONAL_OPERATOR_NOT_CONTAINS:
        return RelationalOperator.RELATIONAL_OPERATOR_NOT_CONTAINS;
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
      default:
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
