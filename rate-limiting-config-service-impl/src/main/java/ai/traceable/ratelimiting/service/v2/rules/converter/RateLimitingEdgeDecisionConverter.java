package ai.traceable.ratelimiting.service.v2.rules.converter;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.edge.decision.config.service.VariableConstants.USER_ATTRIBUTION_VARIABLE_NAME;
import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT;
import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionType.EDGE_DECISION_TYPE_BLOCK;
import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionType.EDGE_DECISION_TYPE_MARK_FOR_TESTING;
import static ai.traceable.edge.decision.config.service.v1.EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST;
import static ai.traceable.edge.decision.config.service.v1.ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_COUNT;
import static ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder.getEncodedRateLimitViolationInfo;
import static ai.traceable.ratelimiting.config.service.v2.Action.MatchCategory.MATCH_CATEGORY_REQUEST;
import static ai.traceable.ratelimiting.config.service.v2.Action.MatchCategory.MATCH_CATEGORY_RESPONSE;
import static ai.traceable.ratelimiting.config.service.v2.ApiAggregateType.API_AGGREGATE_TYPE_ACROSS_ENDPOINTS;
import static ai.traceable.ratelimiting.config.service.v2.ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT;
import static ai.traceable.ratelimiting.config.service.v2.UserAggregateType.USER_AGGREGATE_TYPE_ACROSS_USERS;
import static ai.traceable.ratelimiting.config.service.v2.UserAggregateType.USER_AGGREGATE_TYPE_PER_USER;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingScopeConditionConverter.ENDPOINT_ID;
import static ai.traceable.ratelimiting.service.v2.rules.shared.RateLimitingRulesEdgeDecisionFilter.findAnyMatchingEdgeDecisionAction;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.SpanAttributeHandler;
import ai.traceable.edge.decision.config.service.v1.AggregateThresholdRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecision;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleStatus;
import ai.traceable.edge.decision.config.service.v1.EnvironmentScope;
import ai.traceable.edge.decision.config.service.v1.PayloadDecoration;
import ai.traceable.edge.decision.config.service.v1.RequestHeaderInjection;
import ai.traceable.edge.decision.config.service.v1.ResponseHeaderInjection;
import ai.traceable.edge.decision.config.service.v1.ValueAggregateThreshold;
import ai.traceable.edge.decision.config.service.v1.ValueAggregateThreshold.AggregationType;
import ai.traceable.edge.decision.config.service.v1.ValueAggregateThreshold.ThresholdOperator;
import ai.traceable.platform.actor.v1.RateLimitCategory;
import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig;
import ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig.ValueType;
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
      // only rate limiting rules are currently converted to edge decision rules
      if (!data.getCategory().equals(Category.CATEGORY_RATE_LIMITING)) {
        return Collections.emptyList();
      }
      final EdgeDecisionRuleStatus edgeDecisionRuleStatus = buildRuleStatus(data);
      final Optional<EdgeDecisionRuleScope> maybeEdgeDecisionRuleScope = buildRuleScope(data);
      final Optional<MatchConditionVariablesTuple> mayBeMatchCondition =
          data.hasCondition()
              ? Optional.of(buildMatchCondition(requestContext, data.getCondition()))
              : Optional.empty();
      return getThresholdActionConfigs(data)
          .flatMap(
              action -> {
                EdgeDecision edgeDecision = buildEdgeDecision(rateLimitingRule, action);
                return getResourceAccessThresholdConfigsList(action)
                    .flatMap(
                        resourceAccessThresholdConfig -> {
                          List<EdgeDecisionRuleDefinition> ruleDefinitions =
                              buildRuleDefinitions(
                                  resourceAccessThresholdConfig, mayBeMatchCondition);
                          return ruleDefinitions.stream()
                              .map(
                                  ruleDefinition -> {
                                    EdgeDecisionRule.Builder builder =
                                        EdgeDecisionRule.newBuilder();
                                    builder.setId(rateLimitingRule.getId());
                                    builder.setName(data.getName());
                                    builder.setRuleStatus(edgeDecisionRuleStatus);
                                    builder.setRuleCategory(EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT);
                                    maybeEdgeDecisionRuleScope.ifPresent(builder::setRuleScope);
                                    builder.setRuleDecision(edgeDecision);
                                    builder.setRuleDefinition(ruleDefinition);
                                    return builder.build();
                                  });
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
    Optional<Action> mayBeAction = findAnyMatchingEdgeDecisionAction(thresholdActionConfig);
    if (mayBeAction.isEmpty()) {
      // should never happen
      throw new IllegalArgumentException("unable to find relevant action in threshold config");
    }

    Action action = mayBeAction.get();
    EdgeDecision.Builder builder = EdgeDecision.newBuilder();
    builder.setEdgeDecisionType(
        action.hasBlock() ? EDGE_DECISION_TYPE_BLOCK : EDGE_DECISION_TYPE_MARK_FOR_TESTING);
    if (action.hasMarkForTesting()) {
      builder.addAllDecorations(buildPayloadDecorations(action));
    }
    builder.addAllSpanAttributes(
        SpanAttributeHandler.getSpanAttributeDecorations(
            rateLimitingRule.getId(),
            false,
            EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT,
            getEncodedRateLimitViolationInfo(
                "",
                rateLimitingRule.getId(),
                rateLimitingRule.getData().getName(),
                RateLimitCategory.forNumber(rateLimitingRule.getData().getCategory().getNumber()),
                rateLimitingRule.getData().getLabelsMap())));
    return builder.build();
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
                                    .setOutputType(FIELD_TYPE_STR)
                                    .setStaticValue(
                                        Value.newBuilder()
                                            .setStringValue(headerInjection.getHeaderName())))
                            .setHeaderValue(
                                DataTransformationConfig.newBuilder()
                                    .setOutputType(FIELD_TYPE_STR)
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
                                    .setOutputType(FIELD_TYPE_STR)
                                    .setStaticValue(
                                        Value.newBuilder()
                                            .setStringValue(headerInjection.getHeaderName())))
                            .setHeaderValue(
                                DataTransformationConfig.newBuilder()
                                    .setOutputType(FIELD_TYPE_STR)
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

  private List<EdgeDecisionRuleDefinition> buildRuleDefinitions(
      final ResourceAccessThresholdConfig resourceAccessThresholdConfig,
      final Optional<MatchConditionVariablesTuple> mayBeMatchCondition) {
    return buildAggregateThresholdRules(resourceAccessThresholdConfig, mayBeMatchCondition).stream()
        .map(
            aggregateThresholdRule -> {
              final EdgeDecisionRuleDefinition.Builder builder =
                  EdgeDecisionRuleDefinition.newBuilder();
              builder.setEdgeInputKind(EDGE_INPUT_KIND_HTTP_REQUEST);
              builder.setAggregateThresholdRule(aggregateThresholdRule);
              mayBeMatchCondition.ifPresent(
                  tuple -> builder.addAllRuleVariables(tuple.getVariableDerivationMappings()));
              return builder.build();
            })
        .collect(Collectors.toList());
  }

  @SneakyThrows
  private List<AggregateThresholdRule> buildAggregateThresholdRules(
      final ResourceAccessThresholdConfig resourceAccessThresholdConfig,
      final Optional<MatchConditionVariablesTuple> mayBeMatchCondition) {
    final List<ValueAggregateThreshold> valueAggregateThresholds =
        buildValueAggregateThresholds(
            resourceAccessThresholdConfig,
            mayBeMatchCondition.map(MatchConditionVariablesTuple::getMatchCondition));
    final java.time.Duration duration =
        java.time.Duration.parse(
            resourceAccessThresholdConfig.getRollingWindowThresholdConfig().getDurationIso());
    final Duration timeWindow =
        Duration.newBuilder()
            .setSeconds(duration.getSeconds())
            .setNanos(duration.getNano())
            .build();
    return valueAggregateThresholds.stream()
        .map(
            valueAggregateThreshold -> {
              final AggregateThresholdRule.Builder builder = AggregateThresholdRule.newBuilder();
              mayBeMatchCondition.ifPresent(
                  tuple -> builder.setMatchCondition(tuple.getMatchCondition()));
              builder.addAllGroupByDimensions(
                  buildGroupByDimensions(resourceAccessThresholdConfig));
              buildAggregateWithoutGrouping(resourceAccessThresholdConfig)
                  .ifPresent(builder::setAggregateWithoutGrouping);
              builder.setValueAggregateThreshold(valueAggregateThreshold);
              builder.setTimeWindow(timeWindow);
              return builder.build();
            })
        .collect(Collectors.toList());
  }

  private Stream<ThresholdActionConfig> getThresholdActionConfigs(final RateLimitingRuleData data) {
    return data.getThresholdActionConfigsList().stream()
        .filter(
            thresholdActionConfig ->
                findAnyMatchingEdgeDecisionAction(thresholdActionConfig).isPresent());
  }

  private Stream<ResourceAccessThresholdConfig> getResourceAccessThresholdConfigsList(
      final ThresholdActionConfig thresholdActionConfig) {
    return thresholdActionConfig.getResourceAccessThresholdConfigsList().stream()
        .filter(ResourceAccessThresholdConfig::hasRollingWindowThresholdConfig);
  }

  private MatchConditionVariablesTuple buildMatchCondition(
      final RequestContext requestContext, final Condition condition) {
    switch (condition.getConditionCase()) {
      case LEAF_CONDITION:
        final LeafCondition leafCondition = condition.getLeafCondition();
        final RateLimitingConditionConverter conditionConverter =
            getConditionConverter(leafCondition.getConditionCase());
        final List<VariableDerivationMapping> variableDerivationMappings =
            conditionConverter.buildVariableDerivationMapping(requestContext, leafCondition);
        final MatchCondition matchCondition =
            conditionConverter.buildMatchCondition(requestContext, leafCondition);
        return new MatchConditionVariablesTuple(variableDerivationMappings, matchCondition);
      case COMPOSITE_CONDITION:
        final CompositeCondition compositeCondition = condition.getCompositeCondition();
        final List<VariableDerivationMapping> compositeConditionVariableDerivationMappings =
            new ArrayList<>();
        final List<MatchCondition> childMatchConditions = new ArrayList<>();
        compositeCondition
            .getChildrenList()
            .forEach(
                childCondition -> {
                  final MatchConditionVariablesTuple tuple =
                      this.buildMatchCondition(requestContext, childCondition);
                  compositeConditionVariableDerivationMappings.addAll(
                      tuple.getVariableDerivationMappings());
                  childMatchConditions.add(tuple.getMatchCondition());
                });
        final LogicalMatchCondition.Builder builder = LogicalMatchCondition.newBuilder();
        builder.addAllConditions(childMatchConditions);
        builder.setOperator(convertOperator(compositeCondition.getOperator()));
        final MatchCondition compostiveMatchCondition =
            MatchCondition.newBuilder().setLogicalMatchCondition(builder).build();
        return new MatchConditionVariablesTuple(
            compositeConditionVariableDerivationMappings, compostiveMatchCondition);
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
                      .setTransformationConfig(
                          DataTransformationConfig.newBuilder()
                              .setOutputType(FIELD_TYPE_STR)
                              .setJexlExpression(
                                  JexlExpressionConfig.newBuilder()
                                      .setJexlExpression(USER_ATTRIBUTION_VARIABLE_NAME.name()))))
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
                      .setTransformationConfig(
                          DataTransformationConfig.newBuilder()
                              .setOutputType(FIELD_TYPE_STR)
                              .setJexlExpression(
                                  JexlExpressionConfig.newBuilder()
                                      .setJexlExpression(ENDPOINT_ID))))
              .build());
    }
    return attributeDerivationMappings;
  }

  private Optional<Boolean> buildAggregateWithoutGrouping(
      ResourceAccessThresholdConfig resourceAccessThresholdConfig) {
    if (resourceAccessThresholdConfig
            .getApiAggregateType()
            .equals(API_AGGREGATE_TYPE_ACROSS_ENDPOINTS)
        && resourceAccessThresholdConfig
            .getUserAggregateType()
            .equals(USER_AGGREGATE_TYPE_ACROSS_USERS)) {
      return Optional.of(true);
    }
    return Optional.empty();
  }

  private List<ValueAggregateThreshold> buildValueAggregateThresholds(
      final ResourceAccessThresholdConfig resourceAccessThresholdConfig,
      final Optional<MatchCondition> mayBeMatchCondition) {
    List<ValueAggregateThreshold> valueAggregateThresholds = new ArrayList<>();
    switch (resourceAccessThresholdConfig.getThresholdConfigCase()) {
      case ROLLING_WINDOW_THRESHOLD_CONFIG:
        final ValueAggregateThreshold.Builder builder = ValueAggregateThreshold.newBuilder();
        builder.setAggregationType(AGGREGATION_TYPE_COUNT);
        builder.setStaticThreshold(
            resourceAccessThresholdConfig.getRollingWindowThresholdConfig().getCountAllowed());
        valueAggregateThresholds.add(builder.build());
        break;
      case VALUE_BASED_THRESHOLD_CONFIG:
        ValueType valueType =
            resourceAccessThresholdConfig.getValueBasedThresholdConfig().getValueType();
        List<String> jexlExpressions = getJexlExpressions(valueType, mayBeMatchCondition);

        for (String jexlExpression : jexlExpressions) {
          final ValueAggregateThreshold.Builder valueBasedBuilder =
              ValueAggregateThreshold.newBuilder();
          valueBasedBuilder.setDimension(
              AttributeDerivationMapping.newBuilder()
                  .setName("DISTINCT_COUNT_" + valueType.name())
                  .addRules(
                      DerivationRule.newBuilder()
                          .setTransformationConfig(
                              DataTransformationConfig.newBuilder()
                                  .setOutputType(FIELD_TYPE_STR)
                                  .setJexlExpression(
                                      JexlExpressionConfig.newBuilder()
                                          .setJexlExpression(jexlExpression)))));
          valueBasedBuilder.setAggregationType(AggregationType.AGGREGATION_TYPE_DISTINCT_COUNT);
          valueBasedBuilder.setStaticThreshold(
              resourceAccessThresholdConfig
                  .getValueBasedThresholdConfig()
                  .getUniqueValuesAllowed());
          valueBasedBuilder.setThresholdOperator(ThresholdOperator.THRESHOLD_OPERATOR_ABOVE);
          valueAggregateThresholds.add(valueBasedBuilder.build());
        }
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported resource access threshold case: "
                + resourceAccessThresholdConfig.getThresholdConfigCase());
    }
    return valueAggregateThresholds;
  }

  private RateLimitingConditionConverter getConditionConverter(final ConditionCase conditionCase) {
    return Optional.ofNullable(conditionConverterMap.get(conditionCase))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "No matching condition converter found for condition case: " + conditionCase));
  }

  private List<String> getJexlExpressions(
      final ValueType valueType, final Optional<MatchCondition> mayBeMatchCondition) {
    switch (valueType) {
      case VALUE_TYPE_REQUEST_BODY:
        return Collections.singletonList("$s.getRequestBody()");
      case VALUE_TYPE_SENSITIVE_PARAMS:
        // we don't support conversion of sensitive params condition yet.
        throw new IllegalArgumentException(
            "Cannot convert enumeration rule for which the condition is specified on sensitive param");
      case VALUE_TYPE_PATH_PARAMS:
        // currently we will support the case where the rule is defined for 1 endpoint only.
        Optional<StructuredMatchCondition> maybeParamPathCondition =
            mayBeMatchCondition.flatMap(this::getParamPathCondition);
        if (maybeParamPathCondition.isPresent()) {
          String urlRegex = maybeParamPathCondition.get().getBinaryOperator().getRegex();
          // replace ith occurrence of ".*" in urlRegex with "(.*)" and add it to jexlExpressions
          // list.
          List<String> jexlExpressions = new ArrayList<>();
          int index = -1;
          // find and replace each occurrence of ".*" with "(.*)" and create corresponding jexl
          // expressions
          while ((index = urlRegex.indexOf(".*", index + 1)) != -1) {
            String urlRegexCaptureGroup =
                urlRegex.substring(0, index) + "(.*)" + urlRegex.substring(index + 2);
            // add jexl expression for capturing the path param
            jexlExpressions.add("$s.getPath().replaceFirst(" + urlRegexCaptureGroup + ", $1)");
          }
          return jexlExpressions;
        }
        throw new IllegalArgumentException(
            "Cannot convert enumeration rule where the path param condition doesn't have any url pattern");
      case VALUE_TYPE_UNSPECIFIED:
      default:
        throw new IllegalArgumentException("Unsupported value type: " + valueType);
    }
  }

  private Optional<StructuredMatchCondition> getParamPathCondition(MatchCondition matchCondition) {
    switch (matchCondition.getConditionCase()) {
      case STRUCTURED_MATCH_CONDITION:
        List<DerivationRule> derivationRules =
            matchCondition.getStructuredMatchCondition().getLhs().getRulesList();
        if (derivationRules.size() == 1
            &&
            // this condition should be the same as the transformation done in
            // RateLimitingScopeConditionConverter
            derivationRules
                .get(0)
                .getTransformationConfig()
                .getJexlExpression()
                .getJexlExpression()
                .equals("$s.getPath()")) {
          return Optional.of(matchCondition.getStructuredMatchCondition());
        }
        return Optional.empty();
      case LOGICAL_MATCH_CONDITION:
        for (MatchCondition childCondition :
            matchCondition.getLogicalMatchCondition().getConditionsList()) {
          Optional<StructuredMatchCondition> maybeParamPathCondition =
              getParamPathCondition(childCondition);
          if (maybeParamPathCondition.isPresent()) {
            return maybeParamPathCondition;
          }
        }
        return Optional.empty();
      case GENERIC_MATCH_CONDITION:
      case CONDITION_NOT_SET:
      default:
        return Optional.empty();
    }
  }

  @lombok.Value
  private static class MatchConditionVariablesTuple {
    List<VariableDerivationMapping> variableDerivationMappings;
    MatchCondition matchCondition;
  }
}
