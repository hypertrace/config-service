package ai.traceable.localprocessing.config.service.spanprocessingrules.excludespanrules;

import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRule;
import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRuleInfo;
import ai.traceable.localprocessing.config.service.v1.LogicalOperator;
import ai.traceable.localprocessing.config.service.v1.RelationalOperator;
import ai.traceable.localprocessing.config.service.v1.SpanFilter;
import ai.traceable.localprocessing.config.service.v1.SpanFilterValue;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.utils.SpanFilterMatcher;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleDetails;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.LogicalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.RelationalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;

@Slf4j
public class DefaultExcludeSpanRulesManager implements ExcludeSpanRulesManager {

  private static final String URL_SPAN_ATTRIBUTE_KEY = "http.url";
  private final SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
      configServiceBlockingStub;
  private final SpanFilterMatcher spanFilterMatcher;

  @Inject
  public DefaultExcludeSpanRulesManager(
      SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
          configServiceBlockingStub,
      SpanFilterMatcher spanFilterMatcher) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.spanFilterMatcher = spanFilterMatcher;
  }

  public List<ExcludeSpanProcessingRule> getAllExcludeSpanProcessingRules(
      String serviceName, Optional<String> environment) {
    return configServiceBlockingStub
        .getAllExcludeSpanRules(GetAllExcludeSpanRulesRequest.newBuilder().build())
        .getRuleDetailsList()
        .stream()
        .map(ExcludeSpanRuleDetails::getRule)
        .collect(Collectors.toUnmodifiableList())
        .stream()
        .map(excludeSpanRule -> convertExcludeSpanRule(excludeSpanRule, serviceName, environment))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toUnmodifiableList());
  }

  // assumption: first class field conditions are ANDed and appear in the first level of the filter
  // tree structure
  private Optional<ExcludeSpanProcessingRule> convertExcludeSpanRule(
      ExcludeSpanRule excludeSpanRule, String serviceName, Optional<String> environment) {
    // check if the rule is disabled
    if (!excludeSpanRule.getRuleInfo().hasFilter() || excludeSpanRule.getRuleInfo().getDisabled()) {
      return Optional.empty();
    }

    // apply environment filters if any
    if (!spanFilterMatcher.matchesEnvironment(
        excludeSpanRule.getRuleInfo().getFilter(), environment)) {
      return Optional.empty();
    }

    // apply service name filters if any
    if (!spanFilterMatcher.matchesServiceName(
        excludeSpanRule.getRuleInfo().getFilter(), serviceName)) {
      return Optional.empty();
    }

    Optional<SpanFilter> spanFilter = convertFilter(excludeSpanRule.getRuleInfo().getFilter());
    if (spanFilter.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(
        ExcludeSpanProcessingRule.newBuilder()
            .setExcludeSpanProcessingRuleInfo(
                ExcludeSpanProcessingRuleInfo.newBuilder()
                    .setId(excludeSpanRule.getId())
                    .setFilter(spanFilter.get())
                    .build())
            .build());
  }

  private Optional<SpanFilter> convertFilter(
      org.hypertrace.span.processing.config.service.v1.SpanFilter filter) {
    SpanFilter.Builder filterBuilder = SpanFilter.newBuilder();
    if (filter.hasLogicalSpanFilter()) {
      ai.traceable.localprocessing.config.service.v1.LogicalSpanFilterExpression
          logicalSpanFilterExpression = convertLogicalFilter(filter.getLogicalSpanFilter());
      if (logicalSpanFilterExpression.getOperandsCount() == 0) {
        return Optional.empty();
      } else if (logicalSpanFilterExpression.getOperandsCount() == 1) {
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
                .map(this::convertFilter)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toUnmodifiableList()))
        .build();
  }

  private Optional<ai.traceable.localprocessing.config.service.v1.RelationalSpanFilterExpression>
      convertRelationalFilter(RelationalSpanFilterExpression relationalSpanFilter) {
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
      org.hypertrace.span.processing.config.service.v1.RelationalOperator relationalOperator) {
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
      org.hypertrace.span.processing.config.service.v1.LogicalOperator logicalOperator) {
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
