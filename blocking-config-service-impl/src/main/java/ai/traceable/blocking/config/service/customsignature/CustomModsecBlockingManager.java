package ai.traceable.blocking.config.service.customsignature;

import ai.traceable.blocking.config.service.v1.CustomModsecBlockingRules;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface CustomModsecBlockingManager {

  CustomModsecBlockingRules getEnabledBlockingRules(
      RequestContext requestContext, String requestHash);
}
