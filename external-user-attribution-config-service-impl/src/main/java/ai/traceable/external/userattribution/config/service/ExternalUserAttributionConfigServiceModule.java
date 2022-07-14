package ai.traceable.external.userattribution.config.service;

import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class ExternalUserAttributionConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ExternalUserAttributionConfigServiceConfig
      externalUserAttributionConfigServiceConfig;

  ExternalUserAttributionConfigServiceModule(Channel channel, Config config) {
    this.channel = channel;
    this.externalUserAttributionConfigServiceConfig =
        new ExternalUserAttributionConfigServiceConfig(config);
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(ExternalUserAttributionConfigServiceImpl.class);
    bind(ExternalUserAttributionConfigServiceConfig.class)
        .toInstance(externalUserAttributionConfigServiceConfig);
  }

  @Provides
  UserAttributionConfigServiceBlockingStub providesUserAttributionStub() {
    return UserAttributionConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
