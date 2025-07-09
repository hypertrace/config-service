package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import ai.traceable.detection.exclusion.config.service.v1.BulkUpsertDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.CreateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint;
import ai.traceable.detection.exclusion.config.service.v1.UpdateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.UpsertDetectionExclusionRuleData;
import ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.ExclusionEdgeDecisionRulesSupportChecker;
import ai.traceable.detection.exclusion.config.service.v1.rules.modsec.ExclusionModsecRulesSupportChecker;
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

    // check for edge
    if (!ruleInfo.getExclusionTargetsList().isEmpty()) {
      boolean hasSupportedEdgeDecisionConditions =
          ruleInfo.getConditionsList().stream()
              .allMatch(ExclusionEdgeDecisionRulesSupportChecker::isEdgeDecisionConditionSupported);
      boolean hasSupportedEdgeDecisionExclusionTargets =
          ruleInfo.getExclusionTargetsList().stream()
              .allMatch(ExclusionEdgeDecisionRulesSupportChecker::isEdgeDecisionTargetSupported);
      if (hasSupportedEdgeDecisionConditions && hasSupportedEdgeDecisionExclusionTargets) {
        ruleEvaluationPoints.add(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE);
      }
    }

    // check for agent
    if (!ruleInfo.getConditionsList().isEmpty() && !ruleInfo.getExclusionTargetsList().isEmpty()) {
      boolean hasSupportedModsecConditions =
          ruleInfo.getConditionsList().stream()
              .allMatch(ExclusionModsecRulesSupportChecker::isModsecConditionSupported);
      boolean hasSupportedModsecExclusionTargets =
          ruleInfo.getExclusionTargetsList().stream()
              .allMatch(ExclusionModsecRulesSupportChecker::isModsecExclusionTargetSupported);
      if (hasSupportedModsecConditions && hasSupportedModsecExclusionTargets) {
        ruleEvaluationPoints.add(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT);
      }
    }
    return ruleEvaluationPoints;
  }
}
