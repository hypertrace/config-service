package ai.traceable.localprocessing.config.service.spanprocessingrules.ratelimitconfig;

import com.google.inject.AbstractModule;
import com.typesafe.config.Config;

public class RateLimitConfigManagerModule extends AbstractModule {
  private final Config config;

  public RateLimitConfigManagerModule(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(RateLimitConfigManager.class).to(DefaultRateLimitConfigManager.class);
  }
}
