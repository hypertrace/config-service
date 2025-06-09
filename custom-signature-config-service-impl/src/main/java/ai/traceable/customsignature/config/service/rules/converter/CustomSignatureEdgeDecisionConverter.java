package ai.traceable.customsignature.config.service.rules.converter;

import static ai.traceable.customsignature.config.service.v1.MatchCategory.MATCH_CATEGORY_REQUEST;
import static ai.traceable.customsignature.config.service.v1.MatchCategory.MATCH_CATEGORY_RESPONSE;
import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.edge.decision.config.service.RuleInfoDecorationsHandler.LABELS;
import static ai.traceable.edge.decision.config.service.RuleInfoDecorationsHandler.SEVERITY;
import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_CUSTOM_SIGNATURE;
import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionType.EDGE_DECISION_TYPE_ALERT;
import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionType.EDGE_DECISION_TYPE_BLOCK;
import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionType.EDGE_DECISION_TYPE_MARK_FOR_TESTING;
import static ai.traceable.edge.decision.config.service.v1.EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

import ai.traceable.customsignature.config.service.rules.CustomSignatureRulesEdgeDecisionFilter;
import ai.traceable.customsignature.config.service.rules.converter.expression.CustomSignatureExpressionConverter;
import ai.traceable.customsignature.config.service.v1.AgentModification;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.RuleEffectWithModifications;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.edge.decision.config.service.RuleInfoDecorationsHandler;
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
import ai.traceable.edge.decision.config.service.v1.PolicyKind;
import ai.traceable.edge.decision.config.service.v1.RequestHeaderInjection;
import ai.traceable.edge.decision.config.service.v1.ResponseHeaderInjection;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomSignatureEdgeDecisionConverter {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final Map<Clause.ClauseCase, CustomSignatureExpressionConverter> expressionConverters;

  @Inject
  public CustomSignatureEdgeDecisionConverter(
      Set<CustomSignatureExpressionConverter> expressionConverters) {
    this.expressionConverters =
        expressionConverters.stream()
            .collect(
                collectingAndThen(
                    toMap(CustomSignatureExpressionConverter::getClauseCase, identity()),
                    Maps::immutableEnumMap));
  }

  // caller should only pass custom signature rules that are convertible.
  public EdgeDecisionEngineConfig convert(List<CustomSignatureRule> customSignatureRules) {
    EdgeDecisionEngineConfig.Builder builder = EdgeDecisionEngineConfig.newBuilder();
    List<EdgeDecisionRule> edgeDecisionRules =
        customSignatureRules.stream()
            .map(this::convertCustomSignatureRule)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toUnmodifiableList());
    builder.addAllDecisionRules(edgeDecisionRules);
    return builder.build();
  }

  private Optional<EdgeDecisionRule> convertCustomSignatureRule(
      final CustomSignatureRule customSignatureRule) {
    try {
      EdgeDecisionRule.Builder builder = EdgeDecisionRule.newBuilder();
      final Optional<MatchCondition> mayBeMatchCondition =
          buildMatchCondition(customSignatureRule.getDefinition().getClauseGroup());
      builder.setId(customSignatureRule.getId());
      builder.setPolicyId(customSignatureRule.getId());
      builder.setPolicyKind(PolicyKind.POLICY_KIND_WAF);
      builder.setName(customSignatureRule.getName());
      builder.setRuleStatus(buildRuleStatus(customSignatureRule));
      builder.setRuleCategory(EDGE_DECISION_RULE_CATEGORY_CUSTOM_SIGNATURE);
      buildRuleScope(customSignatureRule).ifPresent(builder::setRuleScope);
      builder.setRuleDecision(buildEdgeDecision(customSignatureRule));
      builder.setRuleDefinition(buildRuleDefinition(mayBeMatchCondition));
      return Optional.of(builder.build());
    } catch (Exception e) {
      log.error("Unable to convert custom signature rule: {}", customSignatureRule, e);
      return Optional.empty();
    }
  }

  private EdgeDecisionRuleStatus buildRuleStatus(CustomSignatureRule customSignatureRule) {
    EdgeDecisionRuleStatus.Builder builder = EdgeDecisionRuleStatus.newBuilder();
    builder.setDisabled(customSignatureRule.getDisabled());
    builder.setInternal(customSignatureRule.getInternal());
    return builder.build();
  }

  private Optional<EdgeDecisionRuleScope> buildRuleScope(CustomSignatureRule customSignatureRule) {
    RuleScope scope = customSignatureRule.getRuleScope();
    EdgeDecisionRuleScope.Builder builder = EdgeDecisionRuleScope.newBuilder();
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

  private Optional<MatchCondition> buildMatchCondition(ClauseGroup clauseGroup) {
    List<MatchCondition> matchConditions =
        clauseGroup.getClausesList().stream()
            .map(this::buildMatchCondition)
            .collect(Collectors.toUnmodifiableList());
    if (matchConditions.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        MatchCondition.newBuilder()
            .setLogicalMatchCondition(
                LogicalMatchCondition.newBuilder()
                    .setOperator(convertOperator(clauseGroup.getClauseOperator()))
                    .addAllConditions(matchConditions)
                    .build())
            .build());
  }

  private LogicalMatchOperator convertOperator(final ClauseOperator operator) {
    switch (operator) {
      case CLAUSE_OPERATOR_AND:
        return LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND;
      case CLAUSE_OPERATOR_OR:
        return LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR;
      default:
        throw new IllegalArgumentException("Unknown operator: " + operator);
    }
  }

  private MatchCondition buildMatchCondition(Clause clause) {
    if (clause.hasClauseGroup()) {
      Optional<MatchCondition> matchCondition = buildMatchCondition(clause.getClauseGroup());
      return matchCondition.orElseThrow(
          () ->
              new IllegalArgumentException(
                  "Failed to build match condition for nested clause group"));
    }
    return getConditionConverter(clause.getClauseCase()).buildMatchCondition(clause);
  }

  private CustomSignatureExpressionConverter getConditionConverter(Clause.ClauseCase clauseCase) {
    return Optional.ofNullable(expressionConverters.get(clauseCase))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "No matching condition converter found for clause case: " + clauseCase));
  }

  private EdgeDecision buildEdgeDecision(CustomSignatureRule customSignatureRule) {
    EdgeDecision.Builder builder = EdgeDecision.newBuilder();
    if (!CustomSignatureRulesEdgeDecisionFilter.hasCompatibleEventType(
        customSignatureRule.getEffect())) {
      throw new IllegalArgumentException(
          "Unsupported rule effect : " + customSignatureRule.getEffect());
    }
    builder.setEdgeDecisionType(convertEventType(customSignatureRule.getEffect().getEventType()));
    builder.addAllDecorations(
        buildPayloadDecorations(customSignatureRule.getEffect().getEffectsList()));

    Map<String, String> ruleInfoDecorations = new HashMap<>();
    ruleInfoDecorations.put(SEVERITY, customSignatureRule.getEffect().getEventSeverity().name());
    try {
      ruleInfoDecorations.put(
          LABELS,
          OBJECT_MAPPER.writeValueAsString(customSignatureRule.getDefinition().getLabelsMap()));
    } catch (JsonProcessingException e) {
      log.error(
          "Error in converting custom signature labels : {} with ruleId : {} to json string",
          customSignatureRule.getDefinition().getLabelsMap(),
          customSignatureRule.getId());
    }

    builder.addAllRuleInfoDecorations(
        RuleInfoDecorationsHandler.getRuleInfoDecorations(ruleInfoDecorations));

    return builder.build();
  }

  private EdgeDecisionType convertEventType(EventType eventType) {
    switch (eventType) {
      case EVENT_TYPE_DETECTION_AND_BLOCKING:
        return EDGE_DECISION_TYPE_BLOCK;
      case EVENT_TYPE_TESTING_DETECTION:
        return EDGE_DECISION_TYPE_MARK_FOR_TESTING;
      case EVENT_TYPE_NORMAL_DETECTION:
        return EDGE_DECISION_TYPE_ALERT;
      case EVENT_TYPE_ALLOW:
        return EdgeDecisionType.EDGE_DECISION_TYPE_ALLOW;
      default:
        throw new IllegalArgumentException("unknown event type : " + eventType);
    }
  }

  private List<PayloadDecoration> buildPayloadDecorations(
      List<RuleEffectWithModifications> ruleEffectWithModifications) {
    return ruleEffectWithModifications.stream()
        .map(RuleEffectWithModifications::getAgentRuleEffect)
        .flatMap(
            agentRuleEffect ->
                agentRuleEffect.getAgentModificationsList().stream()
                    .filter(AgentModification::hasHeaderInjection)
                    .map(AgentModification::getHeaderInjection)
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
                                                        .setStringValue(
                                                            headerInjection.getHeaderName())))
                                        .setHeaderValue(
                                            DataTransformationConfig.newBuilder()
                                                .setOutputType(FIELD_TYPE_STR)
                                                .setStaticValue(
                                                    Value.newBuilder()
                                                        .setStringValue(
                                                            headerInjection
                                                                .getValue()
                                                                .getStaticValue()))))
                                .build();
                          } else if (headerInjection
                              .getHeaderCategory()
                              .equals(MATCH_CATEGORY_RESPONSE)) {
                            return PayloadDecoration.newBuilder()
                                .setResponseHeaderInjection(
                                    ResponseHeaderInjection.newBuilder()
                                        .setHeaderKey(
                                            DataTransformationConfig.newBuilder()
                                                .setOutputType(FIELD_TYPE_STR)
                                                .setStaticValue(
                                                    Value.newBuilder()
                                                        .setStringValue(
                                                            headerInjection.getHeaderName())))
                                        .setHeaderValue(
                                            DataTransformationConfig.newBuilder()
                                                .setOutputType(FIELD_TYPE_STR)
                                                .setStaticValue(
                                                    Value.newBuilder()
                                                        .setStringValue(
                                                            headerInjection
                                                                .getValue()
                                                                .getStaticValue()))))
                                .build();
                          } else {
                            throw new IllegalArgumentException(
                                "unknown header category: " + headerInjection.getHeaderCategory());
                          }
                        }))
        .collect(Collectors.toUnmodifiableList()); // Collect results in a single unmodifiable list
  }

  private EdgeDecisionRuleDefinition buildRuleDefinition(
      Optional<MatchCondition> mayBeMatchCondition) {
    EdgeDecisionRuleDefinition.Builder builder = EdgeDecisionRuleDefinition.newBuilder();
    builder.setEdgeInputKind(EDGE_INPUT_KIND_HTTP_REQUEST);
    mayBeMatchCondition.ifPresent(
        matchCondition -> builder.getSignatureRuleBuilder().setMatchCondition(matchCondition));
    return builder.build();
  }
}
