package ai.traceable.iprange.config.service.rules.migration;

import org.hypertrace.core.grpcutils.context.RequestContext;

public interface IpRangeRulesMigrationManager {
  void migrateFromChangeLog1IfApplicable(RequestContext requestContext);
}
