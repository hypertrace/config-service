package ai.traceable.blocking.config.service.blockingpolicy;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.DataFetcherModule;
import com.google.inject.AbstractModule;

public class BlockingPolicyConfigurationManagerModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(BlockingPolicyConfigurationManager.class)
        .to(DefaultBlockingPolicyConfigurationManager.class);
    install(new DataFetcherModule());
  }
}
