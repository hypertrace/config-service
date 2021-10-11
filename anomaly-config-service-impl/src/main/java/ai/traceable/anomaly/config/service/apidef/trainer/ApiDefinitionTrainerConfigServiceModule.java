package ai.traceable.anomaly.config.service.apidef.trainer;

import com.google.inject.AbstractModule;

public class ApiDefinitionTrainerConfigServiceModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ConfigManager.class).to(ApiDefinitionTrainerConfigManager.class);
  }
}
