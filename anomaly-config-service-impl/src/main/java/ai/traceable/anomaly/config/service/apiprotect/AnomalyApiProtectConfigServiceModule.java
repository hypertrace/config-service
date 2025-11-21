package ai.traceable.anomaly.config.service.apiprotect;

import ai.traceable.anomaly.config.service.AnomalyConfigServiceConfig;
import ai.traceable.anomaly.config.service.apiprotect.protection.engine.ApiProtectEvaluationConfigContextModule;
import ai.traceable.anomaly.config.service.apiprotect.validator.ApiProtectValidator;
import ai.traceable.anomaly.config.service.apiprotect.validator.ApiProtectValidatorImpl;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import io.grpc.BindableService;

public class AnomalyApiProtectConfigServiceModule extends AbstractModule {
  private final String bindableServiceAnnotation;

  public AnomalyApiProtectConfigServiceModule(String bindableServiceAnnotation) {
    this.bindableServiceAnnotation = bindableServiceAnnotation;
  }

  @Override
  protected void configure() {
    bind(BindableService.class)
        .annotatedWith(Names.named(bindableServiceAnnotation))
        .to(AnomalyApiProtectConfigServiceImpl.class);
    bind(ApiProtectValidator.class).to(ApiProtectValidatorImpl.class);
    install(new ApiProtectEvaluationConfigContextModule());
  }

  @Provides
  @Singleton
  ApiProtectConfigServiceConfig providesApiProtectConfigServiceConfig(
      AnomalyConfigServiceConfig config) {
    return new ApiProtectConfigServiceConfig(config.getApiProtectConfigServiceConfig());
  }
}
