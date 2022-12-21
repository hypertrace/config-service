package ai.traceable.integration.config.service;

import static ai.traceable.integration.config.service.IntegrationConfigServiceFactory.SNYK_INTEGRATION_ANNOTATION;

import ai.traceable.integration.config.service.snyk.SnykIntegrationConfigServiceModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
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
    install(new SnykIntegrationConfigServiceModule(SNYK_INTEGRATION_ANNOTATION));
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
