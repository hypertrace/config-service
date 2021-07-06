package ai.traceable.anomaly.config.service.global.validator;

import com.google.inject.AbstractModule;

public class GlobalConfigValidatorModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(GlobalConfigValidator.class).to(AnomalyGlobalConfigServiceValidator.class);
  }
}
