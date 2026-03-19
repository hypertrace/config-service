package ai.traceable.aiapp.protection.config.service.firewall;

import static ai.traceable.aiapp.protection.config.service.firewall.AiAppEvaluationConfigContextModule.AI_APP_CONFIG_CONTEXT_CACHE_PROVIDER;
import static ai.traceable.aiapp.protection.config.service.firewall.AiAppEvaluationConfigContextModule.AI_APP_CONFIG_CONTEXT_CLIENT_PROVIDER;

import ai.traceable.aiapp.protection.config.service.firewall.cache.AiAppConfigContextProvider;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppEvaluationConfigContextRequest;
import ai.traceable.aiapp.protection.config.service.v1.RuleEvaluationPoint;
import ai.traceable.protection.engine.config.aifirewall.v1.AiFirewallConfigContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Singleton
@Slf4j
public class AiAppEvaluationConfigContextManagerImpl
    implements AiAppEvaluationConfigContextManager {
  private final AiAppConfigContextProvider aiAppConfigContextCacheProvider;
  private final AiAppConfigContextProvider aiAppConfigContextClientProvider;

  @Inject
  public AiAppEvaluationConfigContextManagerImpl(
      @Named(AI_APP_CONFIG_CONTEXT_CACHE_PROVIDER)
          AiAppConfigContextProvider aiAppConfigContextCacheProvider,
      @Named(AI_APP_CONFIG_CONTEXT_CLIENT_PROVIDER)
          AiAppConfigContextProvider aiAppConfigContextClientProvider) {
    this.aiAppConfigContextCacheProvider = aiAppConfigContextCacheProvider;
    this.aiAppConfigContextClientProvider = aiAppConfigContextClientProvider;
  }

  @Override
  public AiFirewallConfigContext getAiAppEvaluationConfigContext(
      RequestContext requestContext, GetAiAppEvaluationConfigContextRequest request) {
    RuleEvaluationPoint ruleEvaluationPoint = request.getRuleEvaluationPoint();
    switch (ruleEvaluationPoint) {
      case RULE_EVALUATION_POINT_EDGE:
        return aiAppConfigContextCacheProvider.getAiFirewallConfigContext(requestContext, request);
      case RULE_EVALUATION_POINT_PLATFORM:
        return aiAppConfigContextClientProvider.getAiFirewallConfigContext(requestContext, request);
      default:
        log.warn(
            "Unknown RuleEvaluationPoint: {}. Returning empty config context for requestContext: {}",
            ruleEvaluationPoint,
            requestContext);
        return AiFirewallConfigContext.getDefaultInstance();
    }
  }
}
