package ai.traceable.localprocessing.config.service.customsignature;

import ai.traceable.localprocessing.config.service.v1.CustomModsecDetectionRules;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface CustomModsecDetectionManager {
  CustomModsecDetectionRules getEnabledRules(RequestContext requestContext, String requestHash);

  CustomModsecDetectionRules getEmptyRules();
}
