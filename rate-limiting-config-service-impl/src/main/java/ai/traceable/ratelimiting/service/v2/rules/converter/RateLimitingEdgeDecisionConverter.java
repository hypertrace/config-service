package ai.traceable.ratelimiting.service.v2.rules.converter;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.edge.decision.config.service.VariableConstants.USER_ATTRIBUTION_VARIABLE_NAME;
import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT;
import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionType.EDGE_DECISION_TYPE_ALERT;
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
import ai.traceable.datamodel.data.transformation.config.v1.RegexConfig;
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
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionType;
import ai.traceable.edge.decision.config.service.v1.EnvironmentScope;
import ai.traceable.edge.decision.config.service.v1.PayloadDecoration;
import ai.traceable.edge.decision.config.service.v1.RequestHeaderInjection;
import ai.traceable.edge.decision.config.service.v1.ResponseHeaderInjection;
import ai.traceable.edge.decision.config.service.v1.ValueAggregateThreshold;
import ai.traceable.edge.decision.config.service.v1.ValueAggregateThreshold.AggregationType;
import ai.traceable.edge.decision.config.service.v1.ValueAggregateThreshold.ThresholdOperator;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.platform.actor.v1.RateLimitCategory;
import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.Action.AgentRuleEffect;
import ai.traceable.ratelimiting.config.service.v2.Category;
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
import ai.traceable.ratelimiting.service.v2.rules.converter.condition.MatchConditionDetails;
import ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingConditionConverter;
import ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingScopeConditionConverter;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.protobuf.Duration;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class RateLimitingEdgeDecisionConverter {
  private static final DataTransformationConfig PATH_DATA_TRANSFORMATION_CONFIG =
      DataTransformationConfig.newBuilder()
          .setJexlExpression(JexlExpressionConfig.newBuilder().setJexlExpression("$s.getPath()"))
          .build();
  private static final String REQUEST_BODY_MATCHED_ATTRIBUTE = "http.request.body";
  private static final String PATH_PARAM_MATCHED_ATTRIBUTE_PREFIX = "http.path.param.";

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
      final EdgeDecisionRuleMetadata edgeDecisionRuleMetadata =
          EdgeDecisionRuleMetadata.builder()
              .ruleId(rateLimitingRule.getId())
              .ruleName(data.getName())
              .edgeDecisionRuleStatus(buildRuleStatus(data))
              .maybeEdgeDecisionRuleScope(buildRuleScope(data))
              .category(data.getCategory())
              .labelsMap(data.getLabelsMap())
              .build();
      final Optional<MatchConditionDetails> mayBeMatchCondition =
          data.hasCondition()
              ? Optional.of(buildMatchCondition(requestContext, data.getCondition()))
              : Optional.empty();
      return getThresholdActionConfigs(data)
          .flatMap(
              action ->
                  getResourceAccessThresholdConfigsList(action)
                      .flatMap(
                          resourceAccessThresholdConfig ->
                              buildEdgeDecisionRules(
                                  edgeDecisionRuleMetadata,
                                  action,
                                  resourceAccessThresholdConfig,
                                  mayBeMatchCondition)))
          .collect(Collectors.toUnmodifiableList());
    } catch (Exception ex) {
      log.warn("Unable to convert rate limiting rule: {}", rateLimitingRule, ex);
      return Collections.emptyList();
    }
  }

  private EdgeDecision buildEdgeDecision(
      final EdgeDecisionRuleMetadata edgeDecisionRuleMetadata,
      ThresholdActionConfig thresholdActionConfig,
      ResourceAccessThresholdConfig resourceAccessThresholdConfig,
      ValueAggregateThresholdDetails valueAggregateThresholdDetails) {
    Optional<Action> mayBeAction = findAnyMatchingEdgeDecisionAction(thresholdActionConfig);
    if (mayBeAction.isEmpty()) {
      // should never happen
      throw new IllegalArgumentException("unable to find relevant action in threshold config");
    }

    Action action = mayBeAction.get();
    EdgeDecision.Builder builder = EdgeDecision.newBuilder();
    builder.setEdgeDecisionType(convertAction(action));
    if (action.getMarkForTesting().hasAgentRuleEffect()) {
      builder.addAllDecorations(
          buildPayloadDecorations(action.getMarkForTesting().getAgentRuleEffect()));
    } else if (action.getAlert().hasAgentRuleEffect()) {
      builder.addAllDecorations(buildPayloadDecorations(action.getAlert().getAgentRuleEffect()));
    }
    builder.addAllSpanAttributes(
        SpanAttributeHandler.getSpanAttributeDecorations(
            edgeDecisionRuleMetadata.getRuleId(),
            false,
            EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT,
            getEncodedRateLimitViolationInfo(
                "",
                edgeDecisionRuleMetadata.getRuleId(),
                edgeDecisionRuleMetadata.getRuleName(),
                RateLimitCategory.forNumber(edgeDecisionRuleMetadata.getCategory().getNumber()),
                edgeDecisionRuleMetadata.getLabelsMap()),
            Base64.getEncoder().encodeToString(resourceAccessThresholdConfig.toByteArray()),
            valueAggregateThresholdDetails.getMatchedAttribute()));
    return builder.build();
  }

  private EdgeDecisionType convertAction(Action action) {
    switch (action.getActionCase()) {
      case BLOCK:
        return EDGE_DECISION_TYPE_BLOCK;
      case MARK_FOR_TESTING:
        return EDGE_DECISION_TYPE_MARK_FOR_TESTING;
      case ALERT:
        return EDGE_DECISION_TYPE_ALERT;
      default:
        throw new IllegalArgumentException("unknown action case: " + action.getActionCase());
    }
  }

  private List<PayloadDecoration> buildPayloadDecorations(AgentRuleEffect agentRuleEffect) {
    return agentRuleEffect.getAgentModificationsList().stream()
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

  private Stream<EdgeDecisionRule> buildEdgeDecisionRules(
      final EdgeDecisionRuleMetadata edgeDecisionRuleMetadata,
      final ThresholdActionConfig action,
      final ResourceAccessThresholdConfig resourceAccessThresholdConfig,
      final Optional<MatchConditionDetails> mayBeMatchCondition) {
    final List<ValueAggregateThresholdDetails> valueAggregateThresholdDetailsList =
        buildValueAggregateThresholds(resourceAccessThresholdConfig, mayBeMatchCondition);
    String durationIso = null;
    if (resourceAccessThresholdConfig.hasValueBasedThresholdConfig()) {
      durationIso = resourceAccessThresholdConfig.getValueBasedThresholdConfig().getDurationIso();
    } else if (resourceAccessThresholdConfig.hasRollingWindowThresholdConfig()) {
      durationIso =
          resourceAccessThresholdConfig.getRollingWindowThresholdConfig().getDurationIso();
    } else {
      return Stream.empty();
    }
    final java.time.Duration duration = java.time.Duration.parse(durationIso);
    final Duration timeWindow =
        Duration.newBuilder()
            .setSeconds(duration.getSeconds())
            .setNanos(duration.getNano())
            .build();
    return valueAggregateThresholdDetailsList.stream()
        .map(
            valueAggregateThresholdDetails ->
                buildEdgeDecisionRule(
                    edgeDecisionRuleMetadata,
                    action,
                    resourceAccessThresholdConfig,
                    mayBeMatchCondition,
                    timeWindow,
                    valueAggregateThresholdDetails));
  }

  private EdgeDecisionRule buildEdgeDecisionRule(
      EdgeDecisionRuleMetadata metadata,
      ThresholdActionConfig action,
      final ResourceAccessThresholdConfig resourceAccessThresholdConfig,
      final Optional<MatchConditionDetails> mayBeMatchCondition,
      final Duration timeWindow,
      final ValueAggregateThresholdDetails valueAggregateThresholdDetails) {
    final AggregateThresholdRule.Builder aggregateThresholdRuleBuilder =
        AggregateThresholdRule.newBuilder();
    mayBeMatchCondition.ifPresent(
        tuple -> aggregateThresholdRuleBuilder.setMatchCondition(tuple.getMatchCondition()));
    aggregateThresholdRuleBuilder.addAllGroupByDimensions(
        buildGroupByDimensions(resourceAccessThresholdConfig));
    buildAggregateWithoutGrouping(resourceAccessThresholdConfig)
        .ifPresent(aggregateThresholdRuleBuilder::setAggregateWithoutGrouping);
    aggregateThresholdRuleBuilder.setValueAggregateThreshold(
        valueAggregateThresholdDetails.getValueAggregateThreshold());
    aggregateThresholdRuleBuilder.setTimeWindow(timeWindow);

    final EdgeDecisionRuleDefinition.Builder edgeDecisionRuleDefinitionBuilder =
        EdgeDecisionRuleDefinition.newBuilder();
    edgeDecisionRuleDefinitionBuilder.setEdgeInputKind(EDGE_INPUT_KIND_HTTP_REQUEST);
    edgeDecisionRuleDefinitionBuilder.setAggregateThresholdRule(aggregateThresholdRuleBuilder);
    mayBeMatchCondition.ifPresent(
        tuple ->
            edgeDecisionRuleDefinitionBuilder.addAllRuleVariables(
                tuple.getVariableDerivationMappings()));
    edgeDecisionRuleDefinitionBuilder.addAllRuleVariables(
        valueAggregateThresholdDetails.getVariableDerivationMappings());

    EdgeDecision edgeDecision =
        buildEdgeDecision(
            metadata, action, resourceAccessThresholdConfig, valueAggregateThresholdDetails);

    EdgeDecisionRule.Builder builder = EdgeDecisionRule.newBuilder();
    builder.setId(metadata.getRuleId());
    builder.setName(metadata.getRuleName());
    builder.setRuleStatus(metadata.getEdgeDecisionRuleStatus());
    builder.setRuleCategory(EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT);
    metadata.getMaybeEdgeDecisionRuleScope().ifPresent(builder::setRuleScope);
    builder.setRuleDecision(edgeDecision);
    builder.setRuleDefinition(edgeDecisionRuleDefinitionBuilder);
    return builder.build();
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
        .filter(
            resourceAccessThresholdConfig ->
                resourceAccessThresholdConfig.hasRollingWindowThresholdConfig()
                    || resourceAccessThresholdConfig.hasValueBasedThresholdConfig());
  }

  private MatchConditionDetails buildMatchCondition(
      final RequestContext requestContext, final Condition condition) {
    switch (condition.getConditionCase()) {
      case LEAF_CONDITION:
        final LeafCondition leafCondition = condition.getLeafCondition();
        final RateLimitingConditionConverter conditionConverter =
            getConditionConverter(leafCondition.getConditionCase());
        return conditionConverter.buildMatchCondition(requestContext, leafCondition);
      case COMPOSITE_CONDITION:
        final CompositeCondition compositeCondition = condition.getCompositeCondition();
        final List<VariableDerivationMapping> compositeConditionVariableDerivationMappings =
            new ArrayList<>();
        final List<MatchCondition> childMatchConditions = new ArrayList<>();
        final List<ApiIdentifierEntity> childApiIdentifierEntities = new ArrayList<>();
        compositeCondition
            .getChildrenList()
            .forEach(
                childCondition -> {
                  final MatchConditionDetails matchConditionDetails =
                      this.buildMatchCondition(requestContext, childCondition);
                  compositeConditionVariableDerivationMappings.addAll(
                      matchConditionDetails.getVariableDerivationMappings());
                  childMatchConditions.add(matchConditionDetails.getMatchCondition());
                  childApiIdentifierEntities.addAll(
                      matchConditionDetails.getApiIdentifierEntities());
                });
        final LogicalMatchCondition.Builder builder = LogicalMatchCondition.newBuilder();
        builder.addAllConditions(childMatchConditions);
        builder.setOperator(convertOperator(compositeCondition.getOperator()));
        final MatchCondition compostiveMatchCondition =
            MatchCondition.newBuilder().setLogicalMatchCondition(builder).build();
        return new MatchConditionDetails(
            compostiveMatchCondition,
            compositeConditionVariableDerivationMappings,
            childApiIdentifierEntities.stream()
                .distinct()
                .sorted(Comparator.comparing(ApiIdentifierEntity::getApiId))
                .collect(Collectors.toUnmodifiableList()));
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
                                      .setJexlExpression(
                                          USER_ATTRIBUTION_VARIABLE_NAME.getValue()))))
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

  private List<ValueAggregateThresholdDetails> buildValueAggregateThresholds(
      final ResourceAccessThresholdConfig resourceAccessThresholdConfig,
      final Optional<MatchConditionDetails> mayBeMatchCondition) {
    switch (resourceAccessThresholdConfig.getThresholdConfigCase()) {
      case ROLLING_WINDOW_THRESHOLD_CONFIG:
        final ValueAggregateThreshold.Builder builder = ValueAggregateThreshold.newBuilder();
        builder.setAggregationType(AGGREGATION_TYPE_COUNT);
        builder.setStaticThreshold(
            resourceAccessThresholdConfig.getRollingWindowThresholdConfig().getCountAllowed());
        return List.of(
            ValueAggregateThresholdDetails.builder()
                .valueAggregateThreshold(builder.build())
                .variableDerivationMappings(buildVariableDerivationMappings(mayBeMatchCondition))
                .build());
      case VALUE_BASED_THRESHOLD_CONFIG:
        return buildValueAggregateThresholds(
            resourceAccessThresholdConfig.getValueBasedThresholdConfig(), mayBeMatchCondition);
      default:
        throw new IllegalArgumentException(
            "Unsupported resource access threshold case: "
                + resourceAccessThresholdConfig.getThresholdConfigCase());
    }
  }

  private List<VariableDerivationMapping> buildVariableDerivationMappings(
      Optional<MatchConditionDetails> mayBeMatchCondition) {
    List<VariableDerivationMapping> variableDerivationMappings = new ArrayList<>();
    if (mayBeMatchCondition.isPresent()) {
      MatchConditionDetails matchConditionDetails = mayBeMatchCondition.get();
      List<ApiIdentifierEntity> apiIdentifierEntities =
          matchConditionDetails.getApiIdentifierEntities();
      if (!apiIdentifierEntities.isEmpty()) {
        RateLimitingScopeConditionConverter.buildVariableDerivationMapping(apiIdentifierEntities)
            .ifPresent(variableDerivationMappings::add);
      }
    }
    return variableDerivationMappings;
  }

  private RateLimitingConditionConverter getConditionConverter(final ConditionCase conditionCase) {
    return Optional.ofNullable(conditionConverterMap.get(conditionCase))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "No matching condition converter found for condition case: " + conditionCase));
  }

  private List<ValueAggregateThresholdDetails> buildValueAggregateThresholds(
      final ResourceAccessThresholdConfig.ValueBasedThresholdConfig valueBasedThresholdConfig,
      final Optional<MatchConditionDetails> mayBeMatchCondition) {
    ResourceAccessThresholdConfig.ValueType valueType = valueBasedThresholdConfig.getValueType();
    switch (valueType) {
      case VALUE_TYPE_REQUEST_BODY:
        return List.of(
            ValueAggregateThresholdDetails.builder()
                .valueAggregateThreshold(
                    buildValueAggregateThreshold(valueBasedThresholdConfig, "$s.getRequestBody()"))
                .variableDerivationMappings(buildVariableDerivationMappings(mayBeMatchCondition))
                .matchedAttribute(REQUEST_BODY_MATCHED_ATTRIBUTE)
                .build());
      case VALUE_TYPE_SENSITIVE_PARAMS:
        // we don't support conversion of sensitive params condition yet.
        throw new IllegalArgumentException(
            "Cannot convert enumeration rule for which the condition is specified on sensitive param");
      case VALUE_TYPE_PATH_PARAMS:
        if (mayBeMatchCondition.isPresent()
            && !mayBeMatchCondition.get().getApiIdentifierEntities().isEmpty()) {
          List<ApiIdentifierEntity> apiIdentifierEntities =
              mayBeMatchCondition.get().getApiIdentifierEntities();
          return apiIdentifierEntities.stream()
              .flatMap(
                  apiIdentifierEntity -> {
                    VariableDerivationMapping endPointVariable =
                        RateLimitingScopeConditionConverter.buildVariableDerivationMapping(
                                List.of(apiIdentifierEntity))
                            .get();
                    Map<String, Integer> pathParamIndexes =
                        getPathParamIndexesMap(apiIdentifierEntity);
                    return pathParamIndexes.entrySet().stream()
                        .map(
                            pathParamIndexEntry -> {
                              VariableDerivationMapping pathParamVariable =
                                  VariableDerivationMapping.newBuilder()
                                      .setName("PATH_PARAM_INDEX_" + pathParamIndexEntry.getValue())
                                      .addRules(
                                          DerivationRule.newBuilder()
                                              .addAllTransformationConfigs(
                                                  List.of(
                                                      PATH_DATA_TRANSFORMATION_CONFIG,
                                                      DataTransformationConfig.newBuilder()
                                                          .setRegex(
                                                              RegexConfig.newBuilder()
                                                                  .setSplitRegex("/")
                                                                  .addGroupIndices(
                                                                      pathParamIndexEntry
                                                                          .getValue())
                                                                  .setJoinDelimiter("0"))
                                                          .build())))
                                      .build();
                              return ValueAggregateThresholdDetails.builder()
                                  .valueAggregateThreshold(
                                      buildValueAggregateThreshold(
                                          valueBasedThresholdConfig,
                                          "PATH_PARAM_INDEX_" + pathParamIndexEntry.getValue()))
                                  .variableDerivationMappings(
                                      List.of(endPointVariable, pathParamVariable))
                                  .matchedAttribute(
                                      PATH_PARAM_MATCHED_ATTRIBUTE_PREFIX
                                          + pathParamIndexEntry.getKey())
                                  .build();
                            });
                  })
              .collect(Collectors.toUnmodifiableList());
        }
        throw new IllegalArgumentException(
            "Cannot convert enumeration rule where the path param condition doesn't have any url pattern");
      case VALUE_TYPE_UNSPECIFIED:
      default:
        throw new IllegalArgumentException("Unsupported value type: " + valueType);
    }
  }

  private ValueAggregateThreshold buildValueAggregateThreshold(
      final ResourceAccessThresholdConfig.ValueBasedThresholdConfig valueBasedThresholdConfig,
      final String jexlExpression) {
    final ValueAggregateThreshold.Builder valueBasedBuilder = ValueAggregateThreshold.newBuilder();
    valueBasedBuilder.setDimension(
        AttributeDerivationMapping.newBuilder()
            .setName("DISTINCT_COUNT_" + valueBasedThresholdConfig.getValueType())
            .addRules(
                DerivationRule.newBuilder()
                    .setTransformationConfig(
                        DataTransformationConfig.newBuilder()
                            .setOutputType(FIELD_TYPE_STR)
                            .setJexlExpression(
                                JexlExpressionConfig.newBuilder()
                                    .setJexlExpression(jexlExpression)))));
    valueBasedBuilder.setAggregationType(AggregationType.AGGREGATION_TYPE_DISTINCT_COUNT);
    valueBasedBuilder.setStaticThreshold(valueBasedThresholdConfig.getUniqueValuesAllowed());
    valueBasedBuilder.setThresholdOperator(ThresholdOperator.THRESHOLD_OPERATOR_ABOVE);
    return valueBasedBuilder.build();
  }

  private static Map<String, Integer> getPathParamIndexesMap(
      ApiIdentifierEntity apiIdentifierEntity) {
    Map<String, Integer> pathParamIndexes = new HashMap<>();
    String apiUrlPattern = apiIdentifierEntity.getApiName();
    String[] splitApiUrlPatterns = apiUrlPattern.split("/");
    for (int i = 1; i < splitApiUrlPatterns.length; i++) {
      String splitApiUrlPattern = splitApiUrlPatterns[i];
      if (splitApiUrlPattern.startsWith("{") && splitApiUrlPattern.endsWith("}")) {
        pathParamIndexes.put(
            splitApiUrlPattern.substring(1, splitApiUrlPattern.length() - 1), i + 1);
      }
    }
    if (pathParamIndexes.isEmpty()) {
      throw new IllegalArgumentException(
          "No path params found across configured entity: " + apiIdentifierEntity);
    }
    return pathParamIndexes;
  }

  @lombok.Value
  @Builder
  private static class ValueAggregateThresholdDetails {
    ValueAggregateThreshold valueAggregateThreshold;
    List<VariableDerivationMapping> variableDerivationMappings;
    String matchedAttribute;
  }

  @lombok.Value
  @Builder
  private static class EdgeDecisionRuleMetadata {
    String ruleId;
    String ruleName;
    EdgeDecisionRuleStatus edgeDecisionRuleStatus;
    Optional<EdgeDecisionRuleScope> maybeEdgeDecisionRuleScope;
    Category category;
    Map<String, String> labelsMap;
  }
}
