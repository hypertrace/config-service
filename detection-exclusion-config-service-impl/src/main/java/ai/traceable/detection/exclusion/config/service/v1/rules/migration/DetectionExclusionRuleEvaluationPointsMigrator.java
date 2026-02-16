package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import ai.traceable.detection.exclusion.config.service.v1.BulkUpsertDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.CreateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
import ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint;
import ai.traceable.detection.exclusion.config.service.v1.UpdateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.UpsertDetectionExclusionRuleData;
import ai.traceable.detection.exclusion.config.service.v1.rules.ConditionExcludeChecker;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesValidator;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DetectionExclusionRuleEvaluationPointsMigrator {
  public CreateDetectionExclusionRuleRequest migrateCreateDetectionExclusionRuleRequest(
      CreateDetectionExclusionRuleRequest createDetectionExclusionRuleRequest) {
    return migrateRequest(
        createDetectionExclusionRuleRequest,
        request -> request.hasRuleInfo() ? request.getRuleInfo() : null,
        (request, ruleInfo) -> request.toBuilder().setRuleInfo(ruleInfo).build());
  }

  public UpdateDetectionExclusionRuleRequest migrateUpdateDetectionExclusionRuleRequest(
      UpdateDetectionExclusionRuleRequest updateDetectionExclusionRuleRequest) {
    return migrateRequest(
        updateDetectionExclusionRuleRequest,
        request ->
            request.hasRule() && request.getRule().hasRuleInfo()
                ? request.getRule().getRuleInfo()
                : null,
        (request, ruleInfo) ->
            request.toBuilder()
                .setRule(request.getRule().toBuilder().setRuleInfo(ruleInfo).build())
                .build());
  }

  public BulkUpsertDetectionExclusionRulesRequest migrateBulkUpsertDetectionExclusionRulesRequest(
      BulkUpsertDetectionExclusionRulesRequest bulkUpsertDetectionExclusionRulesRequest) {
    List<UpsertDetectionExclusionRuleData> migratedRules =
        bulkUpsertDetectionExclusionRulesRequest.getRulesList().stream()
            .map(
                ruleData ->
                    migrateRequest(
                        ruleData,
                        data -> data.hasRuleInfo() ? data.getRuleInfo() : null,
                        (data, ruleInfo) -> data.toBuilder().setRuleInfo(ruleInfo).build()))
            .collect(Collectors.toList());
    return bulkUpsertDetectionExclusionRulesRequest.toBuilder()
        .clearRules()
        .addAllRules(migratedRules)
        .build();
  }

  private <T> T migrateRequest(
      T request,
      Function<T, DetectionExclusionRuleInfo> ruleInfoExtractor,
      BiFunction<T, DetectionExclusionRuleInfo, T> requestUpdater) {
    DetectionExclusionRuleInfo detectionExclusionRuleInfo = ruleInfoExtractor.apply(request);
    if (detectionExclusionRuleInfo == null
        || !detectionExclusionRuleInfo.getRuleEvaluationPointsList().isEmpty()) {
      return request;
    }
    List<RuleEvaluationPoint> evaluationPoints =
        getRuleEvaluationPoints(detectionExclusionRuleInfo);
    DetectionExclusionRuleInfo updatedRuleInfo =
        detectionExclusionRuleInfo.toBuilder().addAllRuleEvaluationPoints(evaluationPoints).build();
    return requestUpdater.apply(request, updatedRuleInfo);
  }

  List<RuleEvaluationPoint> getRuleEvaluationPoints(DetectionExclusionRuleInfo ruleInfo) {
    List<RuleEvaluationPoint> ruleEvaluationPoints = new ArrayList<>();
    ruleEvaluationPoints.add(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM); // by default
    List<ExclusionTarget> exclusionTargets = ruleInfo.getExclusionTargetsList();
    List<DetectionExclusionCondition> detectionExclusionConditions = ruleInfo.getConditionsList();

    // check for edge
    if (DetectionExclusionRulesValidator.checkForEdgeDecisionSupportedConditionsAndTargets(
        exclusionTargets, detectionExclusionConditions)) {
      ruleEvaluationPoints.add(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE);
    }

    // check for agent
    // Don't add INLINE_TRACING_AGENT if rule has ALLOW/BLOCK target with exclude conditions enabled
    // since libtraceable doesn't support exclude for these targets yet
    boolean hasExcludeEnabledForAllowOrBlock =
        (exclusionTargets.contains(ExclusionTarget.EXCLUSION_TARGET_ALLOW)
                || exclusionTargets.contains(ExclusionTarget.EXCLUSION_TARGET_BLOCK))
            && ConditionExcludeChecker.hasAnyConditionWithExcludeEnabled(
                detectionExclusionConditions);
    if (DetectionExclusionRulesValidator.checkForModsecSupportedConditionsAndTargets(
            exclusionTargets, detectionExclusionConditions)
        && !hasExcludeEnabledForAllowOrBlock) {
      ruleEvaluationPoints.add(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT);
    }

    return ruleEvaluationPoints;
  }
}
