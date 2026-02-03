package ai.traceable.detection.exclusion.config.service.v1.rules;

import ai.traceable.detection.exclusion.config.service.v1.BulkUpdateDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleRecord;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesResponse;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.UpsertDetectionExclusionRuleData;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesManager {

  List<DetectionExclusionRule> getDetectionExclusionRules(
      RequestContext requestContext, GetRulesFilter filter);

  List<DetectionExclusionRuleRecord> getDetectionExclusionRuleRecords(
      RequestContext requestContext, GetRulesFilter filter);

  DetectionExclusionRule updateDetectionExclusionRule(
      RequestContext requestContext, DetectionExclusionRule rule);

  DetectionExclusionRule createDetectionExclusionRule(
      RequestContext requestContext,
      DetectionExclusionRuleScope ruleScope,
      DetectionExclusionRuleInfo ruleInfo);

  List<DetectionExclusionRule> bulkUpsertDetectionExclusionRule(
      RequestContext requestContext, List<UpsertDetectionExclusionRuleData> ruleDataList);

  void deleteDetectionExclusionRule(RequestContext requestContext, String ruleId);

  GetExclusionModsecRulesResponse getDetectionExclusionModsecRules(
      RequestContext requestContext, GetExclusionModsecRulesRequest request);

  void bulkDeleteDetectionExclusionRules(RequestContext requestContext, List<String> ruleIds);

  void bulkUpdateDetectionExclusionRules(
      RequestContext requestContext, BulkUpdateDetectionExclusionRulesRequest request);
}
