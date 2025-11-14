package ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision;

import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleEvent;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleFamily;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.EventCondition;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
import ai.traceable.detection.exclusion.config.service.v1.LogicalOperator;
import ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition.DetectionExclusionRuleConditionConverter;
import ai.traceable.edge.decision.config.service.v1.EdgeDecision;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleStatus;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleTarget;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionTarget;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionType;
import ai.traceable.edge.decision.config.service.v1.EdgeInputKind;
import ai.traceable.edge.decision.config.service.v1.EnvironmentScope;
import ai.traceable.edge.decision.config.service.v1.PolicyKind;
import ai.traceable.edge.decision.config.service.v1.SignatureRule;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DetectionExclusionRuleEdgeDecisionConverter {

  private final Map<
          DetectionExclusionCondition.ConditionCase, DetectionExclusionRuleConditionConverter>
      conditionConverterMap;

  @Inject
  DetectionExclusionRuleEdgeDecisionConverter(
      Set<DetectionExclusionRuleConditionConverter> conditionConverters) {
    this.conditionConverterMap =
        conditionConverters.stream()
            .collect(
                collectingAndThen(
                    toMap(DetectionExclusionRuleConditionConverter::getConditionCase, identity()),
                    Maps::immutableEnumMap));
  }

  public EdgeDecisionEngineConfig convert(
      RequestContext requestContext, List<DetectionExclusionRule> detectionExclusionRules) {
    List<EdgeDecisionRule> edgeDecisionRules =
        detectionExclusionRules.stream()
            .filter(rule -> hasEdgeRuleEvaluationPoint(rule.getRuleInfo()))
            .map(rule -> getEdgeDecisionRule(requestContext, rule))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toUnmodifiableList());

    return EdgeDecisionEngineConfig.newBuilder().addAllDecisionRules(edgeDecisionRules).build();
  }

  private static boolean hasEdgeRuleEvaluationPoint(
      DetectionExclusionRuleInfo detectionExclusionRuleInfo) {
    return detectionExclusionRuleInfo
        .getRuleEvaluationPointsList()
        .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE);
  }

  private Optional<EdgeDecisionRule> getEdgeDecisionRule(
      RequestContext requestContext, DetectionExclusionRule detectionExclusionRule) {
    try {
      return Optional.of(
          EdgeDecisionRule.newBuilder()
              .setId(detectionExclusionRule.getId())
              .setName(detectionExclusionRule.getRuleInfo().getName())
              .setRuleDecision(
                  getEdgeDecision(
                      detectionExclusionRule.getRuleInfo().getConditionsList(),
                      detectionExclusionRule.getRuleInfo().getExclusionTargetsList()))
              .setPolicyKind(PolicyKind.POLICY_KIND_WAF)
              .setRuleCategory(
                  EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_DETECTION_EXCLUSION)
              .setRuleScope(getEdgeDecisionRuleScope(detectionExclusionRule.getRuleScope()))
              .setRuleDefinition(
                  getEdgeDecisionRuleDefinition(
                      requestContext, detectionExclusionRule.getRuleInfo().getConditionsList()))
              .setRuleStatus(
                  getEdgeDecisionRuleStatus(detectionExclusionRule.getRuleInfo().getRuleStatus()))
              .build());
    } catch (Exception e) {
      log.error(
          "Error in conversion to edge decision rule for detection exclusion rule with id : {}",
          detectionExclusionRule.getId(),
          e);
      return Optional.empty();
    }
  }

  private EdgeDecision getEdgeDecision(
      List<DetectionExclusionCondition> conditions, List<ExclusionTarget> exclusionTargets) {
    List<EventCondition> eventConditions =
        conditions.stream()
            .filter(DetectionExclusionCondition::hasEventCondition)
            .map(DetectionExclusionCondition::getEventCondition)
            .collect(Collectors.toUnmodifiableList());

    return EdgeDecision.newBuilder()
        .setEdgeDecisionType(EdgeDecisionType.EDGE_DECISION_TYPE_EXCLUDE)
        .addAllEdgeDecisionTargets(getEdgeDecisionTargets(eventConditions, exclusionTargets))
        .build();
  }

  private List<EdgeDecisionTarget> getEdgeDecisionTargets(
      List<EventCondition> eventConditions, List<ExclusionTarget> exclusionTargets) {
    return exclusionTargets.stream()
        .map(
            exclusionTarget ->
                EdgeDecisionTarget.newBuilder()
                    .setEdgeDecisionType(getEdgeDecisionType(exclusionTarget))
                    .addAllEdgeDecisionRuleTargets(getEdgeDecisionRuleTargets(eventConditions))
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  private EdgeDecisionType getEdgeDecisionType(ExclusionTarget exclusionTarget) {
    switch (exclusionTarget) {
      case EXCLUSION_TARGET_ALLOW:
        return EdgeDecisionType.EDGE_DECISION_TYPE_ALLOW;
      case EXCLUSION_TARGET_BLOCK:
        return EdgeDecisionType.EDGE_DECISION_TYPE_BLOCK;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported exclusion target : %s", exclusionTarget));
    }
  }

  private List<EdgeDecisionRuleTarget> getEdgeDecisionRuleTargets(
      List<EventCondition> eventConditions) {

    List<SystemDefinedEvent> systemDefinedEvents =
        eventConditions.stream()
            .flatMap(eventCondition -> eventCondition.getSystemDefinedEventsList().stream())
            .collect(Collectors.toUnmodifiableList());

    if (!systemDefinedEvents.isEmpty()) {
      throw new IllegalArgumentException(
          "Exclusion is not supported on system defined events in edge decision service");
    }

    Map<CustomRuleFamily, List<String>> customRuleEvents =
        eventConditions.stream()
            .flatMap(eventCondition -> eventCondition.getCustomRuleEventsList().stream())
            .collect(
                Collectors.toUnmodifiableMap(
                    CustomRuleEvent::getRuleFamily,
                    customRuleEvent ->
                        customRuleEvent.hasRuleId()
                            ? List.of(customRuleEvent.getRuleId())
                            : List.of(),
                    (v1, v2) ->
                        v1.isEmpty() || v2.isEmpty()
                            ? List.of()
                            : Stream.concat(v1.stream(), v2.stream())
                                .collect(Collectors.toUnmodifiableList())));

    List<EdgeDecisionRuleTarget> edgeDecisionRuleTargets = new ArrayList<>();
    for (CustomRuleFamily customRuleFamily : customRuleEvents.keySet()) {
      edgeDecisionRuleTargets.add(
          EdgeDecisionRuleTarget.newBuilder()
              .setEdgeDecisionRuleCategory(getEdgeDecisionRuleCategory(customRuleFamily))
              .addAllRuleIds(customRuleEvents.get(customRuleFamily))
              .build());
    }

    return edgeDecisionRuleTargets;
  }

  private EdgeDecisionRuleCategory getEdgeDecisionRuleCategory(CustomRuleFamily customRuleFamily) {
    if (Objects.requireNonNull(customRuleFamily)
        == CustomRuleFamily.CUSTOM_RULE_FAMILY_RATE_LIMIT) {
      return EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT;
    }
    throw new IllegalArgumentException(
        String.format(
            "Custom rule family : %s not supported for exclusion in edge decision service",
            customRuleFamily));
  }

  private EdgeDecisionRuleScope getEdgeDecisionRuleScope(DetectionExclusionRuleScope ruleScope) {
    if (ruleScope.hasEnvironmentScope()) {
      return EdgeDecisionRuleScope.newBuilder()
          .addScopeConditions(
              EdgeDecisionRuleScopeCondition.newBuilder()
                  .setEnvironmentScope(
                      EnvironmentScope.newBuilder()
                          .addAllEnvironments(
                              ruleScope.getEnvironmentScope().getEnvironmentIdsList())))
          .build();
    }

    return EdgeDecisionRuleScope.getDefaultInstance();
  }

  private EdgeDecisionRuleStatus getEdgeDecisionRuleStatus(DetectionExclusionRuleStatus status) {
    return EdgeDecisionRuleStatus.newBuilder()
        .setDisabled(status.getDisabled())
        .setInternal(status.getGenerateInternalEvents())
        .build();
  }

  private EdgeDecisionRuleDefinition getEdgeDecisionRuleDefinition(
      RequestContext requestContext, List<DetectionExclusionCondition> conditions) {
    return EdgeDecisionRuleDefinition.newBuilder()
        .setEdgeInputKind(EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST)
        .setSignatureRule(
            SignatureRule.newBuilder()
                .setMatchCondition(
                    getMatchCondition(
                        requestContext,
                        conditions,
                        LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND)))
        .build();
  }

  private MatchCondition getMatchCondition(
      RequestContext requestContext,
      List<DetectionExclusionCondition> conditions,
      LogicalMatchOperator logicalMatchOperator) {
    List<MatchCondition> matchConditions =
        conditions.stream()
            .filter(condition -> !condition.hasEventCondition())
            .map(
                condition -> {
                  if (condition.hasConditionalExpression()) {
                    return getMatchCondition(
                        requestContext,
                        condition.getConditionalExpression().getExpressionsList(),
                        getLogicalMatchOperator(
                            condition.getConditionalExpression().getOperator()));
                  } else {
                    return conditionConverterMap
                        .get(condition.getConditionCase())
                        .buildMatchCondition(requestContext, condition);
                  }
                })
            .collect(Collectors.toUnmodifiableList());

    if (matchConditions.size() == 1) {
      return matchConditions.get(0);
    }

    return MatchCondition.newBuilder()
        .setLogicalMatchCondition(
            LogicalMatchCondition.newBuilder()
                .setOperator(logicalMatchOperator)
                .addAllConditions(matchConditions))
        .build();
  }

  private LogicalMatchOperator getLogicalMatchOperator(LogicalOperator logicalOperator) {
    switch (logicalOperator) {
      case LOGICAL_OPERATOR_OR:
        return LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR;
      default:
        // aligning with AND operator being used as the 1st level logical operator
        return LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND;
    }
  }
}
