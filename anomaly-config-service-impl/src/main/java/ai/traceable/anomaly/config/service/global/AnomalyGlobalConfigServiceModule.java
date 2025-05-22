package ai.traceable.anomaly.config.service.global;

import ai.traceable.anomaly.config.service.AnomalyConfigServiceConfig;
import ai.traceable.anomaly.config.service.global.validator.GlobalConfigValidatorModule;
import ai.traceable.anomaly.config.service.global.version.RuleVersionModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import io.grpc.BindableService;

public class AnomalyGlobalConfigServiceModule extends AbstractModule {

  private final String bindableServiceAnnotation;

  public AnomalyGlobalConfigServiceModule(String bindableServiceAnnotation) {
    this.bindableServiceAnnotation = bindableServiceAnnotation;
  }

  @Override
  protected void configure() {
    bind(BindableService.class)
        .annotatedWith(Names.named(bindableServiceAnnotation))
        .to(AnomalyGlobalConfigServiceImpl.class);
    install(new RuleVersionModule());
    install(new GlobalConfigValidatorModule());
  }

  @Provides
  AnomalyGlobalConfigServiceConfig providesAnomalyGlobalServiceConfig(
      AnomalyConfigServiceConfig config) {
    return new AnomalyGlobalConfigServiceConfig(config.getAnomalyGlobalConfig());
  }
}
