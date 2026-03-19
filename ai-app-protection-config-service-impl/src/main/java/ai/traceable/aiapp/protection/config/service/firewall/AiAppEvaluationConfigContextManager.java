package ai.traceable.aiapp.protection.config.service.firewall;

import ai.traceable.aiapp.protection.config.service.v1.GetAiAppEvaluationConfigContextRequest;
import ai.traceable.protection.engine.config.aifirewall.v1.AiFirewallConfigContext;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface AiAppEvaluationConfigContextManager {
  AiFirewallConfigContext getAiAppEvaluationConfigContext(
      RequestContext requestContext, GetAiAppEvaluationConfigContextRequest request);
}
