package ai.traceable.integration.config.service;

import ai.traceable.integration.config.service.snyk.SnykIntegrationConfigServiceImpl;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class IntegrationConfigServiceModule extends AbstractModule {

  private final Channel channel;

  IntegrationConfigServiceModule(Channel channel) {
    this.channel = channel;
  }

  @Override
  protected void configure() {
    Multibinder.newSetBinder(binder(), BindableService.class)
        .addBinding()
        .to(SnykIntegrationConfigServiceImpl.class);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
