package ai.traceable.blocking.config.service.common.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.DataFetcherModule;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;

public class BlockingPolicyConfigurationCommonModule extends AbstractModule {
  private final Config config;

  public BlockingPolicyConfigurationCommonModule(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    install(new DataFetcherModule(config));
  }
}
