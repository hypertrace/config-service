package ai.traceable.customsignature.config.service.rules.provider;

import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextRequest;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConfigContext;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface CustomSignatureConfigContextProvider {
  CustomSignatureConfigContext getCustomSignatureConfigContext(
      RequestContext requestContext, GetCustomSignatureEvaluationConfigContextRequest request);
}
