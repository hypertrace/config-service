package ai.traceable.ratelimiting.service.v2.rules.modsec;

import ai.traceable.anomaly.config.service.registry.AnomalyConfigRegistryModule;
import com.google.inject.AbstractModule;

public class RateLimitingModsecRulesModule extends AbstractModule {
  @Override
  protected void configure() {
    install(new AnomalyConfigRegistryModule());
  }
}
