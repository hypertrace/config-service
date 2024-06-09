package ai.traceable.external.userattribution.config.service;

import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class ExternalUserAttributionConfigServiceModule extends AbstractModule {
  private final Channel channel;

  ExternalUserAttributionConfigServiceModule(Channel channel) {
    this.channel = channel;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(ExternalUserAttributionConfigServiceImpl.class);
  }

  @Provides
  UserAttributionConfigServiceBlockingStub providesUserAttributionStub() {
    return UserAttributionConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  ClientConfig providesClientConfig() {
    return ClientConfig.DEFAULT;
  }
}
