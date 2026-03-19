package ai.traceable.aiapp.protection.config.service.firewall.cache;

import ai.traceable.aiapp.protection.config.service.v1.GetAiAppEvaluationConfigContextRequest;
import ai.traceable.protection.engine.config.aifirewall.v1.AiFirewallConfigContext;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface AiAppConfigContextProvider {
  AiFirewallConfigContext getAiFirewallConfigContext(
      RequestContext requestContext, GetAiAppEvaluationConfigContextRequest request);
}
