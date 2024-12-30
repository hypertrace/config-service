package ai.traceable.ratelimiting.service.v2.rules.converter;

import static ai.traceable.edge.decision.config.service.VariableConstants.USER_ATTRIBUTION_VARIABLE_NAME;
import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT;
import static ai.traceable.edge.decision.config.service.v1.EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST;
import static ai.traceable.edge.decision.config.service.v1.ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_COUNT;
import static ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder.getEncodedRateLimitViolationInfo;
import static ai.traceable.ratelimiting.config.service.v2.Action.ActionCase.BLOCK;
import static ai.traceable.ratelimiting.config.service.v2.Action.ActionCase.MARK_FOR_TESTING;
import static ai.traceable.ratelimiting.config.service.v2.Action.MatchCategory.MATCH_CATEGORY_REQUEST;
import static ai.traceable.ratelimiting.config.service.v2.Action.MatchCategory.MATCH_CATEGORY_RESPONSE;
import static ai.traceable.ratelimiting.config.service.v2.ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT;
import static ai.traceable.ratelimiting.config.service.v2.UserAggregateType.USER_AGGREGATE_TYPE_PER_USER;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.edge.decision.config.service.SpanAttributeHandler;
import ai.traceable.edge.decision.config.service.v1.AggregateThresholdRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecision;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleStatus;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionType;
import ai.traceable.edge.decision.config.service.v1.EnvironmentScope;
import ai.traceable.edge.decision.config.service.v1.LogicalMatchCondition;
import ai.traceable.edge.decision.config.service.v1.LogicalMatchOperator;
import ai.traceable.edge.decision.config.service.v1.MatchCondition;
import ai.traceable.edge.decision.config.service.v1.PayloadDecoration;
import ai.traceable.edge.decision.config.service.v1.RequestHeaderInjection;
import ai.traceable.edge.decision.config.service.v1.ResponseHeaderInjection;
import ai.traceable.edge.decision.config.service.v1.ValueAggregateThreshold;
import ai.traceable.platform.actor.v1.RateLimitCategory;
import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.RuleStatus;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingConditionConverter;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.protobuf.Duration;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class RateLimitingEdgeDecisionConverter {
  private final Map<ConditionCase, RateLimitingConditionConverter> conditionConverterMap;

  @Inject
  public RateLimitingEdgeDecisionConverter(
      final Set<RateLimitingConditionConverter> conditionConverters) {
    this.conditionConverterMap =
        conditionConverters.stream()
            .collect(
                collectingAndThen(
                    toMap(RateLimitingConditionConverter::getConditionCase, identity()),
                    Maps::immutableEnumMap));
  }

  public EdgeDecisionEngineConfig convert(
      final RequestContext requestContext, final List<RateLimitingRule> rateLimitingRules) {
    final EdgeDecisionEngineConfig.Builder builder = EdgeDecisionEngineConfig.newBuilder();
    rateLimitingRules.stream()
        .map(rule -> this.convertRateLimitingRule(requestContext, rule))
        .forEach(builder::addAllDecisionRules);
    return builder.build();
  }

  private List<EdgeDecisionRule> convertRateLimitingRule(
      final RequestContext requestContext, final RateLimitingRule rateLimitingRule) {
    try {
      final RateLimitingRuleData data = rateLimitingRule.getData();
      final EdgeDecisionRuleStatus edgeDecisionRuleStatus = buildRuleStatus(data);
      final Optional<EdgeDecisionRuleScope> maybeEdgeDecisionRuleScope = buildRuleScope(data);
      final Optional<MatchCondition> mayBeMatchCondition =
          data.hasCondition()
              ? Optional.of(buildMatchCondition(requestContext, data.getCondition()))
              : Optional.empty();
      return getThresholdActionConfigs(data)
          .flatMap(
              action -> {
                EdgeDecision edgeDecision = buildEdgeDecision(rateLimitingRule, action);
                return getResourceAccessThresholdConfigsList(action)
                    .map(
                        resourceAccessThresholdConfig -> {
                          EdgeDecisionRule.Builder builder = EdgeDecisionRule.newBuilder();
                          builder.setId(rateLimitingRule.getId());
                          builder.setName(data.getName());
                          builder.setRuleStatus(edgeDecisionRuleStatus);
                          builder.setRuleCategory(EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT);
                          maybeEdgeDecisionRuleScope.ifPresent(builder::setRuleScope);
                          builder.setRuleDecision(edgeDecision);
                          builder.setRuleDefinition(
                              buildRuleDefinition(
                                  resourceAccessThresholdConfig, mayBeMatchCondition));
                          return builder.build();
                        });
              })
          .collect(Collectors.toUnmodifiableList());
    } catch (Exception ex) {
      log.warn("Unable to convert rate limiting rule: {}", rateLimitingRule, ex);
      return Collections.emptyList();
    }
  }

  private EdgeDecision buildEdgeDecision(
      final RateLimitingRule rateLimitingRule, ThresholdActionConfig thresholdActionConfig) {
    Optional<Action> mayBeAction = filterActions(thresholdActionConfig);
    if (mayBeAction.isEmpty()) {
      // should never happen
      throw new IllegalArgumentException("unable to find relevant action in threshold config");
    }

    Action action = mayBeAction.get();
    if (action.hasBlock()) {
      return EdgeDecision.newBuilder()
          .setEdgeDecisionType(EdgeDecisionType.EDGE_DECISION_TYPE_BLOCK)
          .addAllSpanAttributes(
              SpanAttributeHandler.getSpanAttributeDecorations(
                  rateLimitingRule.getId(),
                  false,
                  EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT,
                  getEncodedRateLimitViolationInfo(
                      "",
                      rateLimitingRule.getId(),
                      rateLimitingRule.getData().getName(),
                      RateLimitCategory.forNumber(
                          rateLimitingRule.getData().getCategory().getNumber()),
                      rateLimitingRule.getData().getLabelsMap())))
          .build();
    } else {
      // Not adding any span attributes in this case as the action has no effect on blocking
      return EdgeDecision.newBuilder()
          .setEdgeDecisionType(EdgeDecisionType.EDGE_DECISION_TYPE_ALLOW)
          .addAllDecorations(buildPayloadDecorations(action))
          .build();
    }
  }

  private List<PayloadDecoration> buildPayloadDecorations(Action action) {
    return action.getMarkForTesting().getAgentRuleEffect().getAgentModificationsList().stream()
        .map(Action.AgentModification::getHeaderInjection)
        .map(
            headerInjection -> {
              if (headerInjection.getHeaderCategory().equals(MATCH_CATEGORY_REQUEST)) {
                return PayloadDecoration.newBuilder()
                    .setRequestHeaderInjection(
                        RequestHeaderInjection.newBuilder()
                            .setHeaderKey(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder()
                                            .setStringValue(headerInjection.getHeaderName())))
                            .setHeaderValue(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder()
                                            .setStringValue(
                                                headerInjection.getValue().getStaticValue()))))
                    .build();
              } else if (headerInjection.getHeaderCategory().equals(MATCH_CATEGORY_RESPONSE)) {
                return PayloadDecoration.newBuilder()
                    .setResponseHeaderInjection(
                        ResponseHeaderInjection.newBuilder()
                            .setHeaderKey(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder()
                                            .setStringValue(headerInjection.getHeaderName())))
                            .setHeaderValue(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder()
                                            .setStringValue(
                                                headerInjection.getValue().getStaticValue()))))
                    .build();
              } else {
                throw new IllegalArgumentException(
                    "unknown header category: " + headerInjection.getHeaderCategory());
              }
            })
        .collect(Collectors.toUnmodifiableList());
  }

  private EdgeDecisionRuleStatus buildRuleStatus(final RateLimitingRuleData data) {
    final EdgeDecisionRuleStatus.Builder builder = EdgeDecisionRuleStatus.newBuilder();
    builder.setDisabled(!data.getEnabled());
    RuleStatus status = data.getRuleStatus();
    builder.setInternal(status.getInternal());
    return builder.build();
  }

  private Optional<EdgeDecisionRuleScope> buildRuleScope(final RateLimitingRuleData data) {
    final RuleConfigScope scope = data.getRuleConfigScope();
    final EdgeDecisionRuleScope.Builder builder = EdgeDecisionRuleScope.newBuilder();
    if (scope.hasEnvironmentScope()
        && !scope.getEnvironmentScope().getEnvironmentIdsList().isEmpty()) {
      builder.addScopeConditions(
          EdgeDecisionRuleScopeCondition.newBuilder()
              .setEnvironmentScope(
                  EnvironmentScope.newBuilder()
                      .addAllEnvironments(scope.getEnvironmentScope().getEnvironmentIdsList())));
    }
    return builder.getScopeConditionsList().isEmpty()
        ? Optional.empty()
        : Optional.of(builder.build());
  }

  private EdgeDecisionRuleDefinition buildRuleDefinition(
      final ResourceAccessThresholdConfig resourceAccessThresholdConfig,
      final Optional<MatchCondition> mayBeMatchCondition) {
    final EdgeDecisionRuleDefinition.Builder builder = EdgeDecisionRuleDefinition.newBuilder();
    builder.setEdgeInputKind(EDGE_INPUT_KIND_HTTP_REQUEST);
    builder.setAggregateThresholdRule(
        buildAggregateThresholdRule(resourceAccessThresholdConfig, mayBeMatchCondition));
    return builder.build();
  }

  @SneakyThrows
  private AggregateThresholdRule buildAggregateThresholdRule(
      final ResourceAccessThresholdConfig resourceAccessThresholdConfig,
      final Optional<MatchCondition> mayBeMatchCondition) {
    final AggregateThresholdRule.Builder builder = AggregateThresholdRule.newBuilder();
    mayBeMatchCondition.ifPresent(builder::setMatchCondition);
    builder.addAllGroupByDimensions(buildGroupByDimensions(resourceAccessThresholdConfig));
    builder.setValueAggregateThreshold(buildValueAggregateThreshold(resourceAccessThresholdConfig));
    final java.time.Duration duration =
        java.time.Duration.parse(
            resourceAccessThresholdConfig.getRollingWindowThresholdConfig().getDurationIso());
    builder.setTimeWindow(
        Duration.newBuilder().setSeconds(duration.getSeconds()).setNanos(duration.getNano()));
    return builder.build();
  }

  private Stream<ThresholdActionConfig> getThresholdActionConfigs(final RateLimitingRuleData data) {
    return data.getThresholdActionConfigsList().stream()
        .filter(thresholdActionConfig -> filterActions(thresholdActionConfig).isPresent());
  }

  private Optional<Action> filterActions(ThresholdActionConfig thresholdActionConfig) {
    return thresholdActionConfig.getActionsList().stream()
        .filter(
            action ->
                (action.getActionCase().equals(BLOCK)
                        && action.getBlock().getUseThresholdDuration())
                    || (action.getActionCase().equals(MARK_FOR_TESTING)
                        && action.getMarkForTesting().hasAgentRuleEffect()))
        .findAny();
  }

  private Stream<ResourceAccessThresholdConfig> getResourceAccessThresholdConfigsList(
      final ThresholdActionConfig thresholdActionConfig) {
    return thresholdActionConfig.getResourceAccessThresholdConfigsList().stream()
        .filter(ResourceAccessThresholdConfig::hasRollingWindowThresholdConfig);
  }

  private MatchCondition buildMatchCondition(
      final RequestContext requestContext, final Condition condition) {
    switch (condition.getConditionCase()) {
      case LEAF_CONDITION:
        final LeafCondition leafCondition = condition.getLeafCondition();
        return getConditionConverter(leafCondition.getConditionCase())
            .buildMatchCondition(requestContext, leafCondition);
      case COMPOSITE_CONDITION:
        final CompositeCondition compositeCondition = condition.getCompositeCondition();
        final LogicalMatchCondition.Builder builder = LogicalMatchCondition.newBuilder();
        builder.addAllConditions(
            compositeCondition.getChildrenList().stream()
                .map(childCondition -> this.buildMatchCondition(requestContext, childCondition))
                .collect(Collectors.toUnmodifiableList()));
        builder.setOperator(convertOperator(compositeCondition.getOperator()));
        return MatchCondition.newBuilder().setLogicalMatchCondition(builder).build();
      default:
        throw new IllegalArgumentException(
            "Unknown condition case: " + condition.getConditionCase());
    }
  }

  public LogicalMatchOperator convertOperator(final CompositeCondition.LogicalOperator operator) {
    switch (operator) {
      case LOGICAL_OPERATOR_AND:
        return LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND;
      case LOGICAL_OPERATOR_OR:
        return LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR;
      default:
        throw new IllegalArgumentException("Unknown operator: " + operator);
    }
  }

  private List<AttributeDerivationMapping> buildGroupByDimensions(
      final ResourceAccessThresholdConfig resourceAccessThresholdConfig) {
    final List<AttributeDerivationMapping> attributeDerivationMappings = new ArrayList<>();
    if (resourceAccessThresholdConfig.getUserAggregateType().equals(USER_AGGREGATE_TYPE_PER_USER)) {
      attributeDerivationMappings.add(
          AttributeDerivationMapping.newBuilder()
              .setName("AGGREGATE_PER_USER")
              .addRules(
                  DerivationRule.newBuilder()
                      .setConditionExpression(
                          JexlExpressionConfig.newBuilder()
                              .setJexlExpression(USER_ATTRIBUTION_VARIABLE_NAME.getValue())))
              .build());
    }
    if (resourceAccessThresholdConfig
        .getApiAggregateType()
        .equals(API_AGGREGATE_TYPE_PER_ENDPOINT)) {
      attributeDerivationMappings.add(
          AttributeDerivationMapping.newBuilder()
              .setName("AGGREGATE_PER_API")
              .addRules(
                  DerivationRule.newBuilder()
                      .setConditionExpression(
                          JexlExpressionConfig.newBuilder()
                              .setJexlExpression("$s.getRequestUrl()")))
              .build());
    }
    return attributeDerivationMappings;
  }

  private ValueAggregateThreshold buildValueAggregateThreshold(
      final ResourceAccessThresholdConfig resourceAccessThresholdConfig) {
    final ValueAggregateThreshold.Builder builder = ValueAggregateThreshold.newBuilder();
    builder.setAggregationType(AGGREGATION_TYPE_COUNT);
    switch (resourceAccessThresholdConfig.getThresholdConfigCase()) {
      case ROLLING_WINDOW_THRESHOLD_CONFIG:
        builder.setStaticThreshold(
            resourceAccessThresholdConfig.getRollingWindowThresholdConfig().getCountAllowed());
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported resource access threshold case: "
                + resourceAccessThresholdConfig.getThresholdConfigCase());
    }
    return builder.build();
  }

  private RateLimitingConditionConverter getConditionConverter(final ConditionCase conditionCase) {
    return Optional.ofNullable(conditionConverterMap.get(conditionCase))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "No matching condition converter found for condition case: " + conditionCase));
  }
}
