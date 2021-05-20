package ai.traceable.threatmanagement.config.service.threatautoblocking;

import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionConfig;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ThreatAutoBlockingManager {
  // get threat auto blocking config, if present, else return default security event type
  // contribution
  ThreatAutoBlockingActionConfig getThreatAutoBlockingAction(RequestContext requestContext);

  // update if config present, else create
  ThreatAutoBlockingActionConfig upsertThreatAutoBlockingAction(
      RequestContext requestContext, UpdateThreatAutoBlockingConfigRequest request);
}
