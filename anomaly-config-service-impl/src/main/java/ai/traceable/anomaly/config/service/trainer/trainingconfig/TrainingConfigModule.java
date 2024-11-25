package ai.traceable.anomaly.config.service.trainer.trainingconfig;

import ai.traceable.anomaly.config.service.trainer.trainingconfig.filter.TrainingConfigSpecificFilterModule;
import com.google.inject.AbstractModule;

public class TrainingConfigModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(TrainingConfigManager.class).to(TrainingConfigManagerImpl.class);
    install(new TrainingConfigSpecificFilterModule());
  }
}
