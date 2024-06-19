package ai.traceable.localprocessing.config.service.spanprocessingrules.excludespanrules;

import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanAttributeConstants.URL_PATH_SPAN_ATTRIBUTE_KEYS;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanAttributeConstants.URL_SPAN_ATTRIBUTE_KEYS;

import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRule;
import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRule.Builder;
import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRuleInfo;
import ai.traceable.localprocessing.config.service.v1.ListValue;
import ai.traceable.localprocessing.config.service.v1.LogicalOperator;
import ai.traceable.localprocessing.config.service.v1.RelationalOperator;
import ai.traceable.localprocessing.config.service.v1.SpanFilter;
import ai.traceable.localprocessing.config.service.v1.SpanFilterValue;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.config.span.processing.utils.SpanFilterMatcher;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleDetails;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.LogicalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.RelationalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;

@Slf4j
public class DefaultExcludeSpanRulesManager implements ExcludeSpanRulesManager {

  private static final Set<org.hypertrace.span.processing.config.service.v1.RelationalOperator>
      AGENT_UNSUPPORTED_RELATIONAL_OPERATORS =
          Set.of(
              org.hypertrace.span.processing.config.service.v1.RelationalOperator
                  .RELATIONAL_OPERATOR_IN);
  private final SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
      configServiceBlockingStub;
  private final SpanFilterMatcher spanFilterMatcher;
  private final ClientConfig clientConfig;
  private final Set<String> agentUnsupportedRuleIds;

  @Inject
  public DefaultExcludeSpanRulesManager(
      SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
          configServiceBlockingStub,
      SpanFilterMatcher spanFilterMatcher,
      ClientConfig clientConfig,
      ExcludeSpanRulesConfig excludeSpanRulesConfig) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.spanFilterMatcher = spanFilterMatcher;
    this.clientConfig = clientConfig;
    this.agentUnsupportedRuleIds = excludeSpanRulesConfig.getAgentUnsupportedRuleIds();
  }

  @Override
  public List<ExcludeSpanRule> getAllExcludeSpanRules(RequestContext requestContext) {
    log.debug(
        "Requesting for exclude span processing rules within request context: {} ", requestContext);
    return requestContext
        .call(
            () ->
                configServiceBlockingStub
                    .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getAllExcludeSpanRules(GetAllExcludeSpanRulesRequest.newBuilder().build()))
        .getRuleDetailsList()
        .stream()
        .map(ExcludeSpanRuleDetails::getRule)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<ExcludeSpanProcessingRule> getAllMatchingExcludeSpanProcessingRules(
      RequestContext requestContext,
      List<ExcludeSpanRule> excludeSpanRules,
      String serviceName,
      Optional<String> environment) {
    log.debug(
        "Trying to match exclude span processing rules for service name: {} and environment: {} within request context: {}",
        requestContext,
        serviceName,
        environment);
    return excludeSpanRules.stream()
        .map(excludeSpanRule -> convertExcludeSpanRule(excludeSpanRule, serviceName, environment))
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  // assumption: first class field conditions are ANDed and appear in the first level of the filter
  // tree structure
  private Optional<ExcludeSpanProcessingRule> convertExcludeSpanRule(
      ExcludeSpanRule excludeSpanRule, String serviceName, Optional<String> environment) {
    if (!isSpanFilterPresent(excludeSpanRule)) {
      return Optional.empty();
    }

    if (isAgentUnsupportedRule(excludeSpanRule)) {
      return Optional.empty();
    }

    // check if the rule is disabled
    if (excludeSpanRule.getRuleInfo().getDisabled()) {
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

    try {
      return convertFilter(excludeSpanRule.getRuleInfo().getFilter())
          .map(
              spanFilter ->
                  ExcludeSpanProcessingRule.newBuilder()
                      .setExcludeSpanProcessingRuleInfo(
                          ExcludeSpanProcessingRuleInfo.newBuilder()
                              .setId(excludeSpanRule.getId())
                              .setFilter(spanFilter)))
          .map(Builder::build);
    } catch (Exception e) {
      log.error("Exception occurred in processing spanRule: {}", excludeSpanRule, e);
      return Optional.empty();
    }
  }

  private static boolean isSpanFilterPresent(ExcludeSpanRule excludeSpanRule) {
    return excludeSpanRule.getRuleInfo().hasFilter();
  }

  private boolean isAgentUnsupportedRule(ExcludeSpanRule excludeSpanRule) {
    return isAgentUnsupportedRuleId(excludeSpanRule.getId())
        || isAgentUnsupportedFilter(excludeSpanRule.getRuleInfo().getFilter());
  }

  private boolean isAgentUnsupportedFilter(
      org.hypertrace.span.processing.config.service.v1.SpanFilter spanFilter) {
    switch (spanFilter.getSpanFilterExpressionCase()) {
      case LOGICAL_SPAN_FILTER:
        return isAgentUnsupportedLogicalFilter(spanFilter.getLogicalSpanFilter());
      case RELATIONAL_SPAN_FILTER:
        return isAgentUnsupportedRelationalFilter(spanFilter.getRelationalSpanFilter());
      default:
        return true;
    }
  }

  private boolean isAgentUnsupportedRelationalFilter(
      RelationalSpanFilterExpression relationalSpanFilter) {
    return isAgentUnsupportedRelationalOperator(relationalSpanFilter.getOperator());
  }

  private boolean isAgentUnsupportedLogicalFilter(LogicalSpanFilterExpression logicalSpanFilter) {
    return logicalSpanFilter.getOperandsList().stream().anyMatch(this::isAgentUnsupportedFilter);
  }

  private boolean isAgentUnsupportedRuleId(String ruleId) {
    return agentUnsupportedRuleIds.contains(ruleId);
  }

  private Optional<SpanFilter> convertFilter(
      org.hypertrace.span.processing.config.service.v1.SpanFilter filter) {
    switch (filter.getSpanFilterExpressionCase()) {
      case LOGICAL_SPAN_FILTER:
        return this.convertLogicalFilter(filter.getLogicalSpanFilter());
      case RELATIONAL_SPAN_FILTER:
        return this.convertRelationalFilter(filter.getRelationalSpanFilter());
      default:
        log.warn("Unsupported filter case: {}", filter);
        return Optional.empty();
      case SPANFILTEREXPRESSION_NOT_SET:
        return Optional.empty();
    }
  }

  private Optional<SpanFilter> convertLogicalFilter(LogicalSpanFilterExpression logicalSpanFilter) {
    List<SpanFilter> children =
        logicalSpanFilter.getOperandsList().stream()
            .map(this::convertFilter)
            .flatMap(Optional::stream)
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
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableList());

    return combineFiltersWithOperator(
        individualAttributeFilters, LogicalOperator.LOGICAL_OPERATOR_OR);
  }

  private boolean isAgentUnsupportedRelationalOperator(
      org.hypertrace.span.processing.config.service.v1.RelationalOperator operator) {
    return AGENT_UNSUPPORTED_RELATIONAL_OPERATORS.contains(operator);
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

  private Optional<SpanFilter> buildRelationalFilter(
      String key,
      org.hypertrace.span.processing.config.service.v1.RelationalOperator operator,
      org.hypertrace.span.processing.config.service.v1.SpanFilterValue filterValue) {
    return getSpanFilterValue(filterValue)
        .map(
            spanFilterValue ->
                SpanFilter.newBuilder()
                    .setRelationalFilter(
                        ai.traceable.localprocessing.config.service.v1
                            .RelationalSpanFilterExpression.newBuilder()
                            .setSpanAttributeKey(key)
                            .setOperator(convertRelationalOperator(operator))
                            .setRightOperand(spanFilterValue))
                    .build());
  }

  private Optional<SpanFilterValue> getSpanFilterValue(
      org.hypertrace.span.processing.config.service.v1.SpanFilterValue spanFilterValue) {
    switch (spanFilterValue.getValueCase()) {
      case LIST_VALUE:
        return Optional.of(
            SpanFilterValue.newBuilder()
                .setListValue(getListValue(spanFilterValue.getListValue()))
                .build());
      case STRING_VALUE:
        return Optional.of(
            SpanFilterValue.newBuilder().setStringValue(spanFilterValue.getStringValue()).build());
      default:
        log.error("Unknown spanFilterValue type: {}", spanFilterValue.getValueCase());
        return Optional.empty();
    }
  }

  private ListValue getListValue(
      org.hypertrace.span.processing.config.service.v1.ListValue listValue) {
    List<SpanFilterValue> spanFilterValues =
        listValue.getValuesList().stream()
            .map(this::getSpanFilterValue)
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableList());
    return ListValue.newBuilder().addAllValues(spanFilterValues).build();
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
      org.hypertrace.span.processing.config.service.v1.RelationalOperator relationalOperator) {
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
      case RELATIONAL_OPERATOR_IN:
        return RelationalOperator.RELATIONAL_OPERATOR_IN;
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
