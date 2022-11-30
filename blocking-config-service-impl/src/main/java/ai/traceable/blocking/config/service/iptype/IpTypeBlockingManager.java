package ai.traceable.blocking.config.service.iptype;

import ai.traceable.blocking.config.service.v1.IpTypeBlockingRules;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface IpTypeBlockingManager {
  IpTypeBlockingRules getEnabledBlockingRules(
      RequestContext requestContext, String requestHash, Optional<String> environmentId);
}
