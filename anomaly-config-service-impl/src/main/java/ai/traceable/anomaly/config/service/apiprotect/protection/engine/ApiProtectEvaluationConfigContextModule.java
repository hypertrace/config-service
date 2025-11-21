package ai.traceable.anomaly.config.service.apiprotect.protection.engine;

import ai.traceable.anomaly.config.service.apiprotect.protection.ApiProtectEvaluationConfigContextManager;
import ai.traceable.anomaly.config.service.apiprotect.protection.engine.cache.ApiProtectConfigContextCacheProvider;
import ai.traceable.anomaly.config.service.apiprotect.protection.engine.cache.ApiProtectConfigContextClientProvider;
import ai.traceable.anomaly.config.service.apiprotect.protection.engine.cache.ApiProtectConfigContextProvider;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ApiProtectEvaluationConfigContextModule extends AbstractModule {
  static final String API_PROTECT_CONFIG_CONTEXT_CACHE_PROVIDER =
      "ApiProtectConfigContextCacheProvider";
  static final String API_PROTECT_CONFIG_CONTEXT_CLIENT_PROVIDER =
      "ApiProtectConfigContextClientProvider";

  @Override
  protected void configure() {
    bind(ApiProtectEvaluationConfigContextManager.class)
        .to(ApiProtectEvaluationConfigContextManagerImpl.class);
    bind(ApiProtectConfigContextProvider.class)
        .annotatedWith(Names.named(API_PROTECT_CONFIG_CONTEXT_CACHE_PROVIDER))
        .to(ApiProtectConfigContextCacheProvider.class);
    bind(ApiProtectConfigContextProvider.class)
        .annotatedWith(Names.named(API_PROTECT_CONFIG_CONTEXT_CLIENT_PROVIDER))
        .to(ApiProtectConfigContextClientProvider.class);
  }
}
