package ai.traceable.detection.exclusion.config.service.v1.rules;

import ai.traceable.detection.exclusion.config.service.v1.BulkDeleteDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.BulkUpdateDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.CreateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DeleteDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.GetDetectionExclusionEdgeDecisionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.UpdateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.UpsertDetectionExclusionRuleData;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesValidator {
  void validateOrThrow(RequestContext requestContext, GetDetectionExclusionRulesRequest request);

  void validateOrThrow(
      RequestContext requestContext, GetDetectionExclusionEdgeDecisionRulesRequest request);

  void validateOrThrow(
      RequestContext requestContext,
      UpdateDetectionExclusionRuleRequest request,
      List<DetectionExclusionRule> existingRules);

  void validateOrThrow(
      RequestContext requestContext,
      CreateDetectionExclusionRuleRequest request,
      List<DetectionExclusionRule> existingRules);

  void validateOrThrow(RequestContext requestContext, DeleteDetectionExclusionRuleRequest request);

  void validateOrThrow(RequestContext requestContext, GetExclusionModsecRulesRequest request);

  void validateOrThrowBulkUpsertRequest(
      RequestContext requestContext, List<UpsertDetectionExclusionRuleData> ruleDataList);

  void validateOrThrow(
      RequestContext requestContext, BulkDeleteDetectionExclusionRulesRequest request);

  void validateOrThrow(
      RequestContext requestContext, BulkUpdateDetectionExclusionRulesRequest request);
}
