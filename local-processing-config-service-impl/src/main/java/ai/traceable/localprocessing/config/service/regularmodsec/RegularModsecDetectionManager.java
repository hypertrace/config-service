package ai.traceable.localprocessing.config.service.regularmodsec;

import ai.traceable.localprocessing.config.service.v1.RegularModsecDetectionRules;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RegularModsecDetectionManager {
  RegularModsecDetectionRules getDetectionRules(RequestContext requestContext, String requestHash);

  RegularModsecDetectionRules getEmptyRules();
}
