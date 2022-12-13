package ai.traceable.external.agent.attribute.config.service;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionConfigServiceGrpc;
import ai.traceable.auth.detection.config.service.v1.AuthDetectionConfigServiceGrpc.AuthDetectionConfigServiceBlockingStub;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.external.agent.attribute.config.service.translator.authdetection.AuthDetectionRuleTranslationModule;
import ai.traceable.external.agent.attribute.config.service.translator.userattribution.UserAttributionRuleTranslationModule;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class ExternalAgentAttributeConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final FeatureCachingClient featureCachingClient;

  ExternalAgentAttributeConfigServiceModule(
      Channel channel, FeatureCachingClient featureCachingClient) {
    this.channel = channel;
    this.featureCachingClient = featureCachingClient;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(ExternalAgentAttributeConfigServiceImpl.class);
    bind(FeatureCachingClient.class).toInstance(this.featureCachingClient);
    install(new UserAttributionRuleTranslationModule());
    install(new AuthDetectionRuleTranslationModule());
  }

  @Provides
  UserAttributionConfigServiceBlockingStub providesUserAttributionStub() {
    return UserAttributionConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  AuthDetectionConfigServiceBlockingStub provideAuthDetectionStub() {
    return AuthDetectionConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
