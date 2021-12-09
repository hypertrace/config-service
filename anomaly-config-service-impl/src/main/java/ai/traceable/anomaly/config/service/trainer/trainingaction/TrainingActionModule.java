package ai.traceable.anomaly.config.service.trainer.trainingaction;

import com.google.inject.AbstractModule;
import java.time.Clock;

public class TrainingActionModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(TrainingActionValidator.class).to(TrainingActionValidatorImpl.class);
    bind(TrainingActionManager.class).to(TrainingActionManagerImpl.class);
    bind(Clock.class).toInstance(Clock.systemUTC());
  }
}
