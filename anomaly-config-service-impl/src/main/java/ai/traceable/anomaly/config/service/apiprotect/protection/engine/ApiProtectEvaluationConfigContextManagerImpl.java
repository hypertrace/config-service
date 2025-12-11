package ai.traceable.anomaly.config.service.apiprotect.protection.engine;

import static ai.traceable.anomaly.config.service.apiprotect.protection.engine.ApiProtectEvaluationConfigContextModule.API_PROTECT_CONFIG_CONTEXT_CACHE_PROVIDER;
import static ai.traceable.anomaly.config.service.apiprotect.protection.engine.ApiProtectEvaluationConfigContextModule.API_PROTECT_CONFIG_CONTEXT_CLIENT_PROVIDER;

import ai.traceable.anomaly.config.service.apiprotect.protection.ApiProtectEvaluationConfigContextManager;
import ai.traceable.anomaly.config.service.apiprotect.protection.engine.cache.ApiProtectConfigContextProvider;
import ai.traceable.anomaly.config.service.v1.RuleEvaluationPoint;
import ai.traceable.anomaly.config.service.v1.apiprotect.GetApiProtectEvaluationConfigContextRequest;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.protection.engine.config.apiprotect.v1.ApiProtectionConfigContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Singleton
@Slf4j
public class ApiProtectEvaluationConfigContextManagerImpl
    implements ApiProtectEvaluationConfigContextManager {
  private final FeatureCachingClient featureCachingClient;
  private final ApiProtectConfigContextProvider apiProtectConfigContextCacheProvider;
  private final ApiProtectConfigContextProvider apiProtectConfigContextClientProvider;

  @Inject
  public ApiProtectEvaluationConfigContextManagerImpl(
      FeatureCachingClient featureCachingClient,
      @Named(API_PROTECT_CONFIG_CONTEXT_CACHE_PROVIDER)
          ApiProtectConfigContextProvider apiProtectConfigContextCacheProvider,
      @Named(API_PROTECT_CONFIG_CONTEXT_CLIENT_PROVIDER)
          ApiProtectConfigContextProvider apiProtectConfigContextClientProvider) {
    this.featureCachingClient = featureCachingClient;
    this.apiProtectConfigContextCacheProvider = apiProtectConfigContextCacheProvider;
    this.apiProtectConfigContextClientProvider = apiProtectConfigContextClientProvider;
  }

  @Override
  public ApiProtectionConfigContext getApiProtectEvaluationConfigContext(
      RequestContext requestContext, GetApiProtectEvaluationConfigContextRequest request) {
    if (!featureCachingClient.isProtectionEngineApiProtectEnabledForTenant(requestContext)) {
      return ApiProtectionConfigContext.getDefaultInstance();
    }
    RuleEvaluationPoint ruleEvaluationPoint = request.getRuleEvaluationPoint();
    switch (ruleEvaluationPoint) {
      case RULE_EVALUATION_POINT_EDGE:
        return apiProtectConfigContextCacheProvider.getApiProtectionConfigContext(
            requestContext, request);
      case RULE_EVALUATION_POINT_PLATFORM:
        return apiProtectConfigContextClientProvider.getApiProtectionConfigContext(
            requestContext, request);
      default:
        log.warn(
            "Unknown RuleEvaluationPoint: {}. Returning empty config context for requestContext: {}",
            ruleEvaluationPoint,
            requestContext);
        return ApiProtectionConfigContext.getDefaultInstance();
    }
  }
}
