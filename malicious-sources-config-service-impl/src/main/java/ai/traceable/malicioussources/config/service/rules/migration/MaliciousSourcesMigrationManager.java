package ai.traceable.malicioussources.config.service.rules.migration;

import org.hypertrace.core.grpcutils.context.RequestContext;

public interface MaliciousSourcesMigrationManager {
  void migrateFromChangeLog1IfApplicable(RequestContext requestContext);
}
