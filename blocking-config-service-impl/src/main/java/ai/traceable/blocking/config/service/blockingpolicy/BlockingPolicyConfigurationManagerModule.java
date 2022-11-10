package ai.traceable.blocking.config.service.blockingpolicy;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.DataFetcherModule;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;

public class BlockingPolicyConfigurationManagerModule extends AbstractModule {
  private final Config config;

  public BlockingPolicyConfigurationManagerModule(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(BlockingPolicyConfigurationManager.class)
        .to(DefaultBlockingPolicyConfigurationManager.class);
    install(new DataFetcherModule(config));
  }
}
