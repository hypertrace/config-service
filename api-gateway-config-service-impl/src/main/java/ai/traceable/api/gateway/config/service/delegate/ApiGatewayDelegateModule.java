package ai.traceable.api.gateway.config.service.delegate;

import com.google.inject.AbstractModule;

public class ApiGatewayDelegateModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(ApiRoutesCreator.class).to(ApiRoutesCreatorImpl.class);
    bind(ApiRoutesGetter.class).to(ApiRoutesGetterImpl.class);
    bind(ApiRoutesDeleter.class).to(ApiRoutesDeleterImpl.class);
  }
}
