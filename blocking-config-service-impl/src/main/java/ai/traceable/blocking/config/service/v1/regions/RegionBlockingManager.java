package ai.traceable.blocking.config.service.v1.regions;

import ai.traceable.blocking.config.service.v1.RegionBlockingRules;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RegionBlockingManager {
  RegionBlockingRules getEnabledBlockingRules(
      RequestContext requestContext, String requestHash, Optional<String> environmentId);
}
