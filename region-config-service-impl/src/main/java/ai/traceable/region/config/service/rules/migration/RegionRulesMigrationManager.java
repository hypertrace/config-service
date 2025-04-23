package ai.traceable.region.config.service.rules.migration;

import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RegionRulesMigrationManager {
  void migrateFromChangeLog1IfApplicable(RequestContext requestContext);
}
