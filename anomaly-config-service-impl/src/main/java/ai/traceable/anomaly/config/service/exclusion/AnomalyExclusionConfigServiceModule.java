package ai.traceable.anomaly.config.service.exclusion;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import io.grpc.BindableService;

public class AnomalyExclusionConfigServiceModule extends AbstractModule {
  private final String bindableServiceAnnotation;

  public AnomalyExclusionConfigServiceModule(String bindableServiceAnnotation) {
    this.bindableServiceAnnotation = bindableServiceAnnotation;
  }

  @Override
  protected void configure() {
    bind(BindableService.class)
        .annotatedWith(Names.named(bindableServiceAnnotation))
        .to(AnomalyExclusionConfigServiceImpl.class);
  }
}
