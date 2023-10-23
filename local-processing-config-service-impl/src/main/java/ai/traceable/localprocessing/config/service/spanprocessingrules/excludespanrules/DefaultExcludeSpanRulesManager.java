package ai.traceable.localprocessing.config.service.spanprocessingrules.excludespanrules;

import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRule;
import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRule.Builder;
import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRuleInfo;
import ai.traceable.localprocessing.config.service.v1.LogicalOperator;
import ai.traceable.localprocessing.config.service.v1.RelationalOperator;
import ai.traceable.localprocessing.config.service.v1.SpanFilter;
import ai.traceable.localprocessing.config.service.v1.SpanFilterValue;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
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

  private static final List<String> URL_SPAN_ATTRIBUTE_KEYS =
      List.of("http.url", "http.target", "http.path", "url.full", "url.path");
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

  @Override
  public List<ExcludeSpanRule> getAllExcludeSpanRules(RequestContext requestContext) {
    log.debug(
        "Requesting for exclude span processing rules within request context: {} ", requestContext);
    return requestContext
        .call(
            () ->
                configServiceBlockingStub.getAllExcludeSpanRules(
                    GetAllExcludeSpanRulesRequest.newBuilder().build()))
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

    return convertFilter(excludeSpanRule.getRuleInfo().getFilter())
        .map(
            spanFilter ->
                ExcludeSpanProcessingRule.newBuilder()
                    .setExcludeSpanProcessingRuleInfo(
                        ExcludeSpanProcessingRuleInfo.newBuilder()
                            .setId(excludeSpanRule.getId())
                            .setFilter(spanFilter)))
        .map(Builder::build);
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
      org.hypertrace.span.processing.config.service.v1.RelationalOperator operator,
      org.hypertrace.span.processing.config.service.v1.SpanFilterValue filterValue) {
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
