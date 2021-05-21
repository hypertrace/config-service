package ai.traceable.anomaly.config.service.global.status;

import com.google.inject.AbstractModule;

public class ConfigStatusModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ConfigStatusValidator.class).to(AnomalyGlobalConfigStatusValidator.class);
    bind(ConfigStatusManager.class).to(AnomalyGlobalConfigStatusManager.class);
  }
}
