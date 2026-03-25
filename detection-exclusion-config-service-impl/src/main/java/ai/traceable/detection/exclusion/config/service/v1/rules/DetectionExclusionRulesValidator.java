package ai.traceable.detection.exclusion.config.service.v1.rules;

import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_ALLOW;
import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_BLOCK;
import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_THREAT_ACTOR_CREATION;
import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_THREAT_SCORE_CONTRIBUTION;
import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_UNSPECIFIED;
import static ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE;
import static ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT;
import static ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM;
import static ai.traceable.detection.exclusion.config.service.v1.RuleIntent.RULE_INTENT_UNSPECIFIED;
import static ai.traceable.detection.exclusion.config.service.v1.RuleSource.RULE_SOURCE_DEFAULT;
import static ai.traceable.detection.exclusion.config.service.v1.RuleSource.RULE_SOURCE_UNSPECIFIED;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.detection.exclusion.config.service.v1.BulkDeleteDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.BulkUpdateDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.CreateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DeleteDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition.ConditionCase;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
import ai.traceable.detection.exclusion.config.service.v1.GetDetectionExclusionEdgeDecisionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
import ai.traceable.detection.exclusion.config.service.v1.UpdateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.UpsertDetectionExclusionRuleData;
import ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.ExclusionEdgeDecisionRulesSupportChecker;
import ai.traceable.detection.exclusion.config.service.v1.rules.modsec.ExclusionModsecRulesSupportChecker;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DetectionExclusionRulesValidator implements RulesValidator {

  private final DetectionExclusionConditionValidator conditionValidator;

  @Inject
  public DetectionExclusionRulesValidator(DetectionExclusionConditionValidator conditionValidator) {
    this.conditionValidator = conditionValidator;
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, GetDetectionExclusionRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateFilter(request.getFilter());
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, GetDetectionExclusionEdgeDecisionRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateFilter(request.getFilter());
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      UpdateDetectionExclusionRuleRequest request,
      List<DetectionExclusionRule> existingRules) {
    validateRequestContextOrThrow(requestContext);
    DetectionExclusionRule rule = request.getRule();
    String ruleName = rule.getRuleInfo().getName();
    if (!rule.getRuleInfo()
        .getRuleStatus()
        .getRuleCreationSource()
        .equals(RULE_SOURCE_UNSPECIFIED)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Update request does not allow to update rule creation source for rule with id: %s",
                  rule.getId()))
          .asRuntimeException();
    }
    Optional<DetectionExclusionRule> existingRuleWithSameName =
        getRuleForName(ruleName, existingRules);
    if (existingRuleWithSameName.isPresent()
        && !existingRuleWithSameName.get().getId().equals(rule.getId())) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Rule with name %s already exists", ruleName))
          .asRuntimeException();
    }
    validateNonDefaultPresenceOrThrow(rule, DetectionExclusionRule.ID_FIELD_NUMBER);
    validateRuleInfo(rule.getRuleInfo());
    validateRuleScope(rule.getRuleScope());
  }

  @Override
  public void validateOrThrowBulkUpsertRequest(
      RequestContext requestContext, List<UpsertDetectionExclusionRuleData> ruleDataList) {
    validateRequestContextOrThrow(requestContext);
    ruleDataList.forEach(
        ruleData -> {
          validateRuleInfo(ruleData.getRuleInfo());
          validateRuleScope(ruleData.getRuleScope());
          validateRuleCreationSource(
              ruleData.getRuleInfo().getRuleStatus().getRuleCreationSource());
        });
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      CreateDetectionExclusionRuleRequest request,
      List<DetectionExclusionRule> existingRules) {
    validateRequestContextOrThrow(requestContext);
    validateCreateRequest(request.getRuleInfo(), request.getRuleScope(), existingRules);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, DeleteDetectionExclusionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteDetectionExclusionRuleRequest.ID_FIELD_NUMBER);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, BulkDeleteDetectionExclusionRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, BulkDeleteDetectionExclusionRulesRequest.IDS_FIELD_NUMBER);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, BulkUpdateDetectionExclusionRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request.getIdsList().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Bulk update Detection Exclusion rules request should have at least one id")
          .asRuntimeException();
    }
    if (request.getIdsList().stream().anyMatch(String::isEmpty)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Bulk update Detection Exclusion rules request should not have empty ids")
          .asRuntimeException();
    }
  }

  @VisibleForTesting
  public void validateRuleInfo(DetectionExclusionRuleInfo ruleInfo) {
    validateNonDefaultPresenceOrThrow(ruleInfo, DetectionExclusionRuleInfo.NAME_FIELD_NUMBER);
    validateRuleStatus(ruleInfo.getRuleStatus());
    validateExclusionTargets(ruleInfo.getExclusionTargetsList());
    if (ruleInfo.getConditionsList().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("DetectionExclusionRule has no specified conditions")
          .asRuntimeException();
    }

    if ((ruleInfo.getExclusionTargetsList().contains(EXCLUSION_TARGET_THREAT_SCORE_CONTRIBUTION)
            || ruleInfo.getExclusionTargetsList().contains(EXCLUSION_TARGET_THREAT_ACTOR_CREATION))
        && !ruleInfo.getRuleEvaluationPointsList().isEmpty()
        && !(ruleInfo.getRuleEvaluationPointsList().size() == 1
            && ruleInfo.getRuleEvaluationPointsList().contains(RULE_EVALUATION_POINT_PLATFORM))) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "DetectionExclusionRule must have only RULE_EVALUATION_POINT_PLATFORM evaluation point")
          .asRuntimeException();
    }

    if (ruleInfo.getExclusionTargetsList().stream()
            .anyMatch(
                target ->
                    target.equals(EXCLUSION_TARGET_BLOCK) || target.equals(EXCLUSION_TARGET_ALLOW))
        && ruleInfo.getConditionsList().stream()
                .map(DetectionExclusionCondition::getConditionCase)
                .filter(Predicate.isEqual(ConditionCase.EVENT_CONDITION))
                .count()
            > 1) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "DetectionExclusionRule for blocking supports at max one event condition")
          .asRuntimeException();
    }
    for (DetectionExclusionCondition condition : ruleInfo.getConditionsList()) {
      conditionValidator.validateRuleCondition(ruleInfo.getExclusionTargetsList(), condition);
    }

    validateRuleEvaluationPoints(
        ruleInfo.getRuleEvaluationPointsList(),
        ruleInfo.getExclusionTargetsList(),
        ruleInfo.getConditionsList());
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, GetExclusionModsecRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateFilter(request.getRulesFilter());
  }

  private void validateCreateRequest(
      DetectionExclusionRuleInfo ruleInfo,
      DetectionExclusionRuleScope ruleScope,
      List<DetectionExclusionRule> existingRules) {
    String ruleName = ruleInfo.getName();
    Optional<DetectionExclusionRule> existingRuleWithSameName =
        getRuleForName(ruleName, existingRules);
    if (existingRuleWithSameName.isPresent()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Rule with name %s already exists", ruleName))
          .asRuntimeException();
    }
    validateRuleInfo(ruleInfo);
    validateRuleScope(ruleScope);
    validateRuleCreationSource(ruleInfo.getRuleStatus().getRuleCreationSource());
  }

  private void validateExclusionTargets(List<ExclusionTarget> exclusionTargets) {
    if (!exclusionTargets.isEmpty()
        && (exclusionTargets.stream()
            .anyMatch(exclusionTarget -> exclusionTarget.equals(EXCLUSION_TARGET_UNSPECIFIED)))) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Exclusion targets should not contain UNSPECIFIED exclusion target")
          .asRuntimeException();
    }
  }

  private void validateRuleScope(DetectionExclusionRuleScope scope) {
    if (scope.hasEnvironmentScope()) {
      validateNonDefaultPresenceOrThrow(
          scope.getEnvironmentScope(), EnvironmentScope.ENVIRONMENT_IDS_FIELD_NUMBER);
      if (scope.getEnvironmentScope().getEnvironmentIdsList().stream().anyMatch(String::isEmpty)) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Environment id should not be an empty string")
            .asRuntimeException();
      }
    }
  }

  private void validateRuleStatus(DetectionExclusionRuleStatus status) {
    if (status.hasExpirationDetails()
        && !status.getExpirationDetails().hasExpirationTimestampMillis()
        && !status.getExpirationDetails().hasExpirationDuration()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Expiration details should have expiration timestamp or expiration duration")
          .asRuntimeException();
    }
    if (status.getExpirationDetails().hasExpirationDuration()) {
      try {
        java.time.Duration.parse(status.getExpirationDetails().getExpirationDuration());
      } catch (DateTimeParseException iae) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Expiration duration should be in valid ISO 8601 format")
            .asRuntimeException();
      }
    }
  }

  private void validateRuleCreationSource(RuleSource ruleSource) {
    if (ruleSource.equals(RULE_SOURCE_DEFAULT)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Rule source cannot be set to default while creating a rule")
          .asRuntimeException();
    }
  }

  private Optional<DetectionExclusionRule> getRuleForName(
      String ruleName, List<DetectionExclusionRule> detectionExclusionRules) {
    return detectionExclusionRules.stream()
        .filter(rule -> rule.getRuleInfo().getName().equals(ruleName))
        .findFirst();
  }

  private void validateFilter(GetRulesFilter filter) {
    if (filter.getExclusionTargetsList().contains(EXCLUSION_TARGET_UNSPECIFIED)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Cannot filter for UNSPECIFIED exclusion target")
          .asRuntimeException();
    }
    if (filter.getRuleCreationSourcesList().stream()
        .anyMatch(Predicate.isEqual(RULE_SOURCE_UNSPECIFIED))) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Cannot filter for UNSPECIFIED rule scope")
          .asRuntimeException();
    }
    if (filter.getRuleIntentsList().stream().anyMatch(Predicate.isEqual(RULE_INTENT_UNSPECIFIED))) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Cannot filter for UNSPECIFIED rule intent")
          .asRuntimeException();
    }
  }

  private void validateRuleEvaluationPoints(
      List<RuleEvaluationPoint> ruleEvaluationPoints,
      List<ExclusionTarget> exclusionTargets,
      List<DetectionExclusionCondition> conditions) {
    if (ruleEvaluationPoints.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("The list of RuleEvaluationPoints cannot be empty.")
          .asRuntimeException();
    }

    if (exclusionTargets.size() == 1
        && exclusionTargets.contains(EXCLUSION_TARGET_ALLOW)
        && ruleEvaluationPoints.contains(RULE_EVALUATION_POINT_PLATFORM)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "RULE_EVALUATION_POINT_PLATFORM cannot be one of the rule evaluation points when EXCLUSION_TARGET_ALLOW is the only exclusion target.")
          .asRuntimeException();
    }

    if (ruleEvaluationPoints.contains(RULE_EVALUATION_POINT_EDGE)) {
      if (!checkForEdgeDecisionSupportedConditionsAndTargets(exclusionTargets, conditions)) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "RULE_EVALUATION_POINT_EDGE cannot be one of the rule evaluation points as either exclusionTargets or conditions is unsupported.")
            .asRuntimeException();
      }
    }

    if (ruleEvaluationPoints.contains(RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)) {
      if (!checkForModsecSupportedConditionsAndTargets(exclusionTargets, conditions)) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "RULE_EVALUATION_POINT_INLINE_TRACING_AGENT cannot be one of the rule evaluation points as either exclusionTargets or conditions is unsupported")
            .asRuntimeException();
      }
    }
  }

  public static boolean checkForEdgeDecisionSupportedConditionsAndTargets(
      List<ExclusionTarget> exclusionTargets,
      List<DetectionExclusionCondition> detectionExclusionConditions) {
    if (exclusionTargets.isEmpty() || detectionExclusionConditions.isEmpty()) {
      return false;
    }

    boolean hasSupportedEdgeDecisionConditions =
        detectionExclusionConditions.stream()
            .allMatch(ExclusionEdgeDecisionRulesSupportChecker::isEdgeDecisionConditionSupported);
    boolean hasSupportedEdgeDecisionExclusionTargets =
        exclusionTargets.stream()
            .anyMatch(ExclusionEdgeDecisionRulesSupportChecker::isEdgeDecisionTargetSupported);

    return hasSupportedEdgeDecisionConditions && hasSupportedEdgeDecisionExclusionTargets;
  }

  public static boolean checkForModsecSupportedConditionsAndTargets(
      List<ExclusionTarget> exclusionTargets,
      List<DetectionExclusionCondition> detectionExclusionConditions) {
    if (exclusionTargets.isEmpty() || detectionExclusionConditions.isEmpty()) {
      return false;
    }

    /*
     * The presence of an OR logical operator here as the 1st level list of DetectionExclusionConditions is
     * implicitly ANDed, and the nested LogicalConditionalExpression is anyway checked against in
     * ExclusionModsecRulesSupportChecker.isModsecConditionSupported.
     */
    boolean hasSupportedModsecConditions =
        detectionExclusionConditions.stream()
            .allMatch(ExclusionModsecRulesSupportChecker::isModsecConditionSupported);
    boolean hasSupportedModsecExclusionTargets =
        exclusionTargets.stream()
            .anyMatch(ExclusionModsecRulesSupportChecker::isModsecExclusionTargetSupported);

    return hasSupportedModsecConditions && hasSupportedModsecExclusionTargets;
  }
}
