package ai.traceable.aiapp.protection.config.service.firewall;

import ai.traceable.aiapp.protection.config.service.firewall.cache.AiAppConfigContextCacheProvider;
import ai.traceable.aiapp.protection.config.service.firewall.cache.AiAppConfigContextClientProvider;
import ai.traceable.aiapp.protection.config.service.firewall.cache.AiAppConfigContextProvider;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AiAppEvaluationConfigContextModule extends AbstractModule {
  static final String AI_APP_CONFIG_CONTEXT_CACHE_PROVIDER = "AiAppConfigContextCacheProvider";
  static final String AI_APP_CONFIG_CONTEXT_CLIENT_PROVIDER = "AiAppConfigContextClientProvider";

  @Override
  protected void configure() {
    bind(AiAppEvaluationConfigContextManager.class)
        .to(AiAppEvaluationConfigContextManagerImpl.class);
    bind(AiAppConfigContextProvider.class)
        .annotatedWith(Names.named(AI_APP_CONFIG_CONTEXT_CACHE_PROVIDER))
        .to(AiAppConfigContextCacheProvider.class);
    bind(AiAppConfigContextProvider.class)
        .annotatedWith(Names.named(AI_APP_CONFIG_CONTEXT_CLIENT_PROVIDER))
        .to(AiAppConfigContextClientProvider.class);
  }
}
