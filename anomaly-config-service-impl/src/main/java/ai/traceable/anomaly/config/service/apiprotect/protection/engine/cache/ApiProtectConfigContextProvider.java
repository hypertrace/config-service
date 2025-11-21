package ai.traceable.anomaly.config.service.apiprotect.protection.engine.cache;

import ai.traceable.anomaly.config.service.v1.apiprotect.GetApiProtectEvaluationConfigContextRequest;
import ai.traceable.protection.engine.config.apiprotect.v1.ApiProtectionConfigContext;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ApiProtectConfigContextProvider {
  ApiProtectionConfigContext getApiProtectionConfigContext(
      RequestContext requestContext, GetApiProtectEvaluationConfigContextRequest request);
}
