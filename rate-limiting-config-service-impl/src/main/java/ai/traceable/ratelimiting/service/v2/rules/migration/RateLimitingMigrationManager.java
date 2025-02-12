package ai.traceable.ratelimiting.service.v2.rules.migration;

import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RateLimitingMigrationManager {
  void migrateFromChangeLog1IfApplicable(RequestContext requestContext);
}
