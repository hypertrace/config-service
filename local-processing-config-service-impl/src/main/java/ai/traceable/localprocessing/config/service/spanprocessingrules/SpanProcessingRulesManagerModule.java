package ai.traceable.localprocessing.config.service.spanprocessingrules;

import ai.traceable.localprocessing.config.service.spanprocessingrules.excludespanrules.ExcludeSpanRulesManagerModule;
import ai.traceable.localprocessing.config.service.spanprocessingrules.ratelimitconfig.RateLimitConfigManagerModule;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;

public class SpanProcessingRulesManagerModule extends AbstractModule {

  private final Config config;

  public SpanProcessingRulesManagerModule(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(SpanProcessingRulesManager.class).to(DefaultSpanProcessingRulesManager.class);
    install(new ExcludeSpanRulesManagerModule());
    install(new RateLimitConfigManagerModule(this.config));
  }
}
