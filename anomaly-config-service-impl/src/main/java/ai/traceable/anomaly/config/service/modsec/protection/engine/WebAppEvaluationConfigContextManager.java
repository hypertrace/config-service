package ai.traceable.anomaly.config.service.modsec.protection.engine;

import ai.traceable.anomaly.config.service.v1.modsec.GetWebAppEvaluationConfigContextRequest;
import ai.traceable.protection.engine.config.web.application.v1.WebAppEvaluationConfigContext;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface WebAppEvaluationConfigContextManager {

  WebAppEvaluationConfigContext getWebAppEvaluationConfigContext(
      RequestContext requestContext, GetWebAppEvaluationConfigContextRequest request);
}
