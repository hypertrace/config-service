package ai.traceable.blocking.config.service.v1.customsignature;

import ai.traceable.blocking.config.service.v1.CustomModsecBlockingRules;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface CustomModsecBlockingManager {

  CustomModsecBlockingRules getEnabledBlockingRules(
      RequestContext requestContext, String requestHash, Optional<String> environmentId);
}
