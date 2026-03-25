package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import ai.traceable.detection.exclusion.config.service.v1.BulkUpsertDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.CreateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.UpdateDetectionExclusionRuleRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesMigrationManager {
  void migrateFromOldStoreIfApplicable(RequestContext requestContext);

  void migrateFromChangeLog2IfApplicable(RequestContext requestContext);

  void migrateFromChangeLog3IfApplicable(RequestContext requestContext);

  void migrateFromChangeLog4IfApplicable(RequestContext requestContext);

  void migrateForRuleEvaluationPointsIfApplicable(RequestContext requestContext);

  void migrateForApiProtectionExclusionRulesIfApplicable(RequestContext requestContext);

  void migrateForAllowOnlyPlatformRemovalIfApplicable(RequestContext requestContext);

  void migrateForExclusionTargetAnyMatchFixIfApplicable(RequestContext requestContext);

  CreateDetectionExclusionRuleRequest migrateCreateDetectionExclusionRuleRequest(
      CreateDetectionExclusionRuleRequest createDetectionExclusionRuleRequest);

  UpdateDetectionExclusionRuleRequest migrateUpdateDetectionExclusionRuleRequest(
      UpdateDetectionExclusionRuleRequest updateDetectionExclusionRuleRequest);

  BulkUpsertDetectionExclusionRulesRequest migrateBulkUpsertDetectionExclusionRulesRequest(
      BulkUpsertDetectionExclusionRulesRequest bulkUpsertDetectionExclusionRulesRequest);
}
