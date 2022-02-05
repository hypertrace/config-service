package ai.traceable.anomaly.config.service.global.status;

import com.google.inject.AbstractModule;

public class ConfigStatusModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ConfigStatusManager.class).to(AnomalyGlobalConfigStatusManager.class);
    bind(GlobalAnomalyConfigStatusManager.class).to(GlobalAnomalyConfigStatusManagerImpl.class);
  }
}
