package ai.traceable.anomaly.config.service.trainer.trainingconfig;

import com.google.inject.AbstractModule;

public class TrainingConfigModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(TrainingConfigManager.class).to(TrainingConfigManagerImpl.class);
  }
}
