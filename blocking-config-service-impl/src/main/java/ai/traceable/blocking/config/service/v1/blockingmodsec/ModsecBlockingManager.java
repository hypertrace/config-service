package ai.traceable.blocking.config.service.v1.blockingmodsec;

import ai.traceable.blocking.config.service.v1.SafeCrsBlockingRules;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ModsecBlockingManager {

  SafeCrsBlockingRules getEnabledBlockingRules(
      RequestContext requestContext, String requestHash, Optional<String> environmentId);
}
