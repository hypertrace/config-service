package ai.traceable.anomaly.config.service.modsec;

import ai.traceable.anomaly.config.service.AnomalyConfigServiceConfig;
import ai.traceable.anomaly.config.service.modsec.protection.engine.WebAppEvaluationConfigContextModule;
import ai.traceable.anomaly.config.service.modsec.rules.ModsecModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import io.grpc.BindableService;

public class AnomalyModsecConfigServiceModule extends AbstractModule {
  private final String bindableServiceAnnotation;

  public AnomalyModsecConfigServiceModule(String bindableServiceAnnotation) {
    this.bindableServiceAnnotation = bindableServiceAnnotation;
  }

  @Override
  protected void configure() {
    bind(BindableService.class)
        .annotatedWith(Names.named(bindableServiceAnnotation))
        .to(AnomalyModsecConfigServiceImpl.class);
    install(new ModsecModule());
    install(new WebAppEvaluationConfigContextModule());
  }

  @Provides
  @Singleton
  ModsecConfigServiceConfig providesModsecConfigServiceConfig(AnomalyConfigServiceConfig config) {
    return new ModsecConfigServiceConfig(config.getModsecConfigServiceConfig());
  }
}
