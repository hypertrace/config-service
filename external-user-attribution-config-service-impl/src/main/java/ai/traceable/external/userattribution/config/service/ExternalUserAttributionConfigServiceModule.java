package ai.traceable.external.userattribution.config.service;

import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class ExternalUserAttributionConfigServiceModule extends AbstractModule {
  private final ManagedChannel channel;
  private final ExternalUserAttributionConfigServiceConfig
      externalUserAttributionConfigServiceConfig;
  private final ExternalUserAttributionRuleTranslator externalUserAttributionRuleTranslator;

  ExternalUserAttributionConfigServiceModule(ManagedChannel channel, Config config) {
    this.channel = channel;
    this.externalUserAttributionConfigServiceConfig =
        new ExternalUserAttributionConfigServiceConfig(config);
    this.externalUserAttributionRuleTranslator =
        new ExternalUserAttributionRuleTranslator(externalUserAttributionConfigServiceConfig);
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(ExternalUserAttributionConfigServiceImpl.class);
    bind(ExternalUserAttributionConfigServiceConfig.class)
        .toInstance(externalUserAttributionConfigServiceConfig);
    bind(ExternalUserAttributionRuleTranslator.class)
        .toInstance(externalUserAttributionRuleTranslator);
  }

  @Provides
  UserAttributionConfigServiceBlockingStub providesUserAttributionStub() {
    return UserAttributionConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
