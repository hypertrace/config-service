package ai.traceable.detection.exclusion.config.service.v1.rules;

import ai.traceable.detection.exclusion.config.service.v1.BulkDeleteDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.CreateDetectionExclusionRuleData;
import ai.traceable.detection.exclusion.config.service.v1.CreateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DeleteDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.GetDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.UpdateDetectionExclusionRuleRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesValidator {
  void validateOrThrow(RequestContext requestContext, GetDetectionExclusionRulesRequest request);

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

  void validateOrThrow(
      RequestContext requestContext, List<CreateDetectionExclusionRuleData> ruleDataList);

  void validateOrThrow(
      RequestContext requestContext, BulkDeleteDetectionExclusionRulesRequest request);
}
