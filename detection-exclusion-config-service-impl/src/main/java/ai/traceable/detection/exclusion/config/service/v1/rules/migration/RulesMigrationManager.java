package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesMigrationManager {
  void migrateFromOldStoreIfApplicable(RequestContext requestContext);

  void migrateFromChangeLog2IfApplicable(RequestContext requestContext);

  void migrateFromChangeLog3IfApplicable(RequestContext requestContext);
}
