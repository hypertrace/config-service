package ai.traceable.integration.config.service;

import ai.traceable.integration.config.service.snyk.SnykIntegrationConfigServiceImpl;
import ai.traceable.integration.config.service.wiz.WizIntegrationConfigServiceImpl;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class IntegrationConfigServiceModule extends AbstractModule {

  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  IntegrationConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    Multibinder<BindableService> multibinder =
        Multibinder.newSetBinder(binder(), BindableService.class);
    multibinder.addBinding().to(SnykIntegrationConfigServiceImpl.class);
    multibinder.addBinding().to(WizIntegrationConfigServiceImpl.class);

    bind(ConfigChangeEventGenerator.class).toInstance(this.configChangeEventGenerator);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
