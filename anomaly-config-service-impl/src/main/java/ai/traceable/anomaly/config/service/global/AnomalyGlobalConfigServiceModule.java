package ai.traceable.anomaly.config.service.global;

import ai.traceable.anomaly.config.service.global.status.ConfigStatusModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import io.grpc.BindableService;

public class AnomalyGlobalConfigServiceModule extends AbstractModule {

  private final Config config;
  private final String bindableServiceAnnotation;

  public AnomalyGlobalConfigServiceModule(Config config, String bindableServiceAnnotation) {
    this.config = config;
    this.bindableServiceAnnotation = bindableServiceAnnotation;
  }

  @Override
  protected void configure() {
    bind(BindableService.class)
        .annotatedWith(Names.named(bindableServiceAnnotation))
        .to(AnomalyGlobalConfigServiceImpl.class);
    install(new ConfigStatusModule());
  }

  @Provides
  AnomalyGlobalConfigServiceConfig providesAnomalyGlobalServiceConfig() {
    return new AnomalyGlobalConfigServiceConfig(this.config);
  }
}
