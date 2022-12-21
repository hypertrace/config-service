package ai.traceable.integration.config.service.snyk;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import io.grpc.BindableService;

public class SnykIntegrationConfigServiceModule extends AbstractModule {

  private final String bindableServiceAnnotation;

  public SnykIntegrationConfigServiceModule(String bindableServiceAnnotation) {
    this.bindableServiceAnnotation = bindableServiceAnnotation;
  }

  @Override
  protected void configure() {
    bind(BindableService.class)
        .annotatedWith(Names.named(bindableServiceAnnotation))
        .to(SnykIntegrationConfigServiceImpl.class);
  }
}
