package ai.traceable.anomaly.config.service.override;

import ai.traceable.anomaly.config.service.override.exclusion.ExclusionRulesManagerModule;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import io.grpc.BindableService;

public class DetectionOverrideConfigServiceModule extends AbstractModule {

  private final String bindableServiceAnnotation;

  public DetectionOverrideConfigServiceModule(String bindableServiceAnnotation) {
    this.bindableServiceAnnotation = bindableServiceAnnotation;
  }

  @Override
  protected void configure() {
    bind(BindableService.class)
        .annotatedWith(Names.named(bindableServiceAnnotation))
        .to(DetectionOverrideConfigServiceImpl.class);
    install(new ExclusionRulesManagerModule());
  }
}
