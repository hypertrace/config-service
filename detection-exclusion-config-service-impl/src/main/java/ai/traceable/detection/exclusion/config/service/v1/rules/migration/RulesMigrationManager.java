package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesMigrationManager {
  boolean shouldMigrateFromOldStore(RequestContext requestContext);

  void updateDetectionExclusionRulesFromOldStore(RequestContext requestContext);
}
