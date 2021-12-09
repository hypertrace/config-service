package ai.traceable.anomaly.config.service.trainer;

import ai.traceable.anomaly.config.service.trainer.trainingaction.TrainingActionModule;
import ai.traceable.anomaly.config.service.trainer.trainingconfig.TrainingConfigModule;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import io.grpc.BindableService;

public class TrainerConfigServiceModule extends AbstractModule {

  private final String bindableServiceAnnotation;

  public TrainerConfigServiceModule(String bindableServiceAnnotation) {
    this.bindableServiceAnnotation = bindableServiceAnnotation;
  }

  @Override
  protected void configure() {
    bind(BindableService.class)
        .annotatedWith(Names.named(bindableServiceAnnotation))
        .to(TrainerConfigServiceImpl.class);
    install(new TrainingConfigModule());
    install(new TrainingActionModule());
  }
}
