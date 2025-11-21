package ai.traceable.anomaly.config.service.apiprotect.protection;

import ai.traceable.anomaly.config.service.v1.apiprotect.GetApiProtectEvaluationConfigContextRequest;
import ai.traceable.protection.engine.config.apiprotect.v1.ApiProtectionConfigContext;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ApiProtectEvaluationConfigContextManager {
  ApiProtectionConfigContext getApiProtectEvaluationConfigContext(
      RequestContext requestContext, GetApiProtectEvaluationConfigContextRequest request);
}
