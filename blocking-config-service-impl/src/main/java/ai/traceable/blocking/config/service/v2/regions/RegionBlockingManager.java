package ai.traceable.blocking.config.service.v2.regions;

import ai.traceable.blocking.config.service.v2.RegionBlockingRules;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RegionBlockingManager {
  RegionBlockingRules getEnabledBlockingRules(
      RequestContext requestContext, Optional<String> environmentId);
}
