package ai.traceable.blocking.config.service.v2.iptype;

import ai.traceable.blocking.config.service.v2.IpTypeBlockingRules;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface IpTypeBlockingManager {
  IpTypeBlockingRules getEnabledBlockingRules(
      RequestContext requestContext, Optional<String> environmentId);
}
