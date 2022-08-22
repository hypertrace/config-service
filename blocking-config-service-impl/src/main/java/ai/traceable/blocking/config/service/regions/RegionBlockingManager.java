package ai.traceable.blocking.config.service.regions;

import ai.traceable.blocking.config.service.v1.RegionBlockingRules;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RegionBlockingManager {
  RegionBlockingRules getEnabledBlockingRules(RequestContext requestContext, String requestHash);
}
