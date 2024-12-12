package ai.traceable.ratelimiting.service.v2.rules.converter;

import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT;
import static ai.traceable.edge.decision.config.service.v1.EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST;
import static ai.traceable.edge.decision.config.service.v1.ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_COUNT;
import static ai.traceable.ratelimiting.config.service.v2.ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT;
import static ai.traceable.ratelimiting.config.service.v2.UserAggregateType.USER_AGGREGATE_TYPE_PER_USER;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
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
import ai.traceable.edge.decision.config.service.v1.ValueAggregateThreshold;
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
import ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingConditionConverter;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.protobuf.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RateLimitingEdgeDecisionConverter {
  private static final EdgeDecision BLOCK_EDGE_DECISION =
      EdgeDecision.newBuilder()
          .setEdgeDecisionType(EdgeDecisionType.EDGE_DECISION_TYPE_BLOCK)
          .build();

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

  public EdgeDecisionEngineConfig convert(final List<RateLimitingRule> rateLimitingRules) {
    final EdgeDecisionEngineConfig.Builder builder = EdgeDecisionEngineConfig.newBuilder();
    rateLimitingRules.stream()
        .map(this::convertRateLimitingRule)
        .forEach(builder::addAllDecisionRules);
    return builder.build();
  }

  private List<EdgeDecisionRule> convertRateLimitingRule(final RateLimitingRule rateLimitingRule) {
    try {
      final RateLimitingRuleData data = rateLimitingRule.getData();
      final EdgeDecisionRuleStatus edgeDecisionRuleStatus = buildRuleStatus(data);
      final Optional<EdgeDecisionRuleScope> maybeEdgeDecisionRuleScope = buildRuleScope(data);
      final Optional<MatchCondition> mayBeMatchCondition =
          data.hasCondition()
              ? Optional.of(buildMatchCondition(data.getCondition()))
              : Optional.empty();
      return getResourceAccessThresholdConfigs(data).stream()
          .map(
              resourceAccessThresholdConfig -> {
                EdgeDecisionRule.Builder builder = EdgeDecisionRule.newBuilder();
                builder.setId(rateLimitingRule.getId());
                builder.setName(data.getName());
                builder.setRuleStatus(edgeDecisionRuleStatus);
                builder.setRuleCategory(EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT);
                maybeEdgeDecisionRuleScope.ifPresent(builder::setRuleScope);
                builder.setRuleDecision(BLOCK_EDGE_DECISION);
                builder.setRuleDefinition(
                    buildRuleDefinition(resourceAccessThresholdConfig, mayBeMatchCondition));
                return builder.build();
              })
          .collect(Collectors.toUnmodifiableList());
    } catch (Exception ex) {
      log.warn("Unable to convert rate limiting rule: {}", rateLimitingRule, ex);
      return Collections.emptyList();
    }
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

  private List<ResourceAccessThresholdConfig> getResourceAccessThresholdConfigs(
      final RateLimitingRuleData data) {
    return data.getThresholdActionConfigsList().stream()
        .filter(
            thresholdActionConfig ->
                thresholdActionConfig.getActionsList().stream()
                    .anyMatch(action -> action.getActionCase().equals(Action.ActionCase.BLOCK)))
        .flatMap(
            thresholdActionConfig ->
                thresholdActionConfig.getResourceAccessThresholdConfigsList().stream())
        .collect(Collectors.toUnmodifiableList());
  }

  private MatchCondition buildMatchCondition(final Condition condition) {
    switch (condition.getConditionCase()) {
      case LEAF_CONDITION:
        final LeafCondition leafCondition = condition.getLeafCondition();
        return getConditionConverter(leafCondition.getConditionCase())
            .buildMatchCondition(leafCondition);
      case COMPOSITE_CONDITION:
        final CompositeCondition compositeCondition = condition.getCompositeCondition();
        final LogicalMatchCondition.Builder builder = LogicalMatchCondition.newBuilder();
        builder.addAllConditions(
            compositeCondition.getChildrenList().stream()
                .map(this::buildMatchCondition)
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
                          JexlExpressionConfig.newBuilder().setJexlExpression("endUserId")))
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
