package ai.traceable.anomaly.config.service.apidef;

import ai.traceable.anomaly.config.service.AnomalyConfigServiceConfig;
import ai.traceable.anomaly.config.service.apidef.trainer.ApiDefinitionTrainerConfigServiceModule;
import ai.traceable.anomaly.config.service.registry.AnomalyConfigRegistryModule;
import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionTrainerConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import io.grpc.BindableService;

public class ApiDefinitionConfigServiceModule extends AbstractModule {

  private final String bindableServiceAnnotation;

  public ApiDefinitionConfigServiceModule(String bindableServiceAnnotation) {
    this.bindableServiceAnnotation = bindableServiceAnnotation;
  }

  @Override
  protected void configure() {
    bind(BindableService.class)
        .annotatedWith(Names.named(bindableServiceAnnotation))
        .to(ApiDefinitionConfigServiceImpl.class);
    install(new AnomalyConfigRegistryModule());
    install(new ApiDefinitionTrainerConfigServiceModule());
  }

  @Provides
  ApiDefinitionTrainerConfig providesApiDefinitionTrainerConfig(AnomalyConfigServiceConfig config) {
    return config.getApiDefinitionTrainerConfig();
  }
}
