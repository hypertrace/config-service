package ai.traceable.external.data.classification.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.obfuscation.config.service.v1.DataObfuscationConfigServiceGrpc;
import ai.traceable.data.obfuscation.config.service.v1.DataObfuscationConfigServiceGrpc.DataObfuscationConfigServiceBlockingStub;
import ai.traceable.data.parsing.config.service.v1.DataParsingConfigServiceGrpc;
import ai.traceable.data.parsing.config.service.v1.DataParsingConfigServiceGrpc.DataParsingConfigServiceBlockingStub;
import ai.traceable.platform.insights.api.v1.InsightsServiceGrpc;
import ai.traceable.platform.insights.api.v1.InsightsServiceGrpc.InsightsServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationConfigServiceGrpc;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationConfigServiceGrpc.SessionIdentificationConfigServiceBlockingStub;
import ai.traceable.userattribution.config.service.v2.UserAttributionConfigServiceGrpc;
import ai.traceable.userattribution.config.service.v2.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class ExternalDataClassificationConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final Config config;
  private final GrpcChannelRegistry channelRegistry;
  private final FeatureCachingClient featureCachingClient;
  private static final String INSIGHTS_SERVICE_CONFIG = "insights.service.config";

  ExternalDataClassificationConfigServiceModule(
      Channel channel,
      Config config,
      GrpcChannelRegistry channelRegistry,
      FeatureCachingClient featureCachingClient) {
    this.channel = channel;
    this.config = config;
    this.channelRegistry = channelRegistry;
    this.featureCachingClient = featureCachingClient;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(ExternalDataClassificationConfigServiceImpl.class);
    bind(ExternalDataClassificationConfig.class)
        .toProvider(() -> new ExternalDataClassificationConfig(this.config));
    bind(FeatureCachingClient.class).toInstance(this.featureCachingClient);
  }

  @Provides
  SessionIdentificationConfigServiceBlockingStub provideSessionIdentificationConfigService() {
    return SessionIdentificationConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  UserAttributionConfigServiceBlockingStub provideUserAttributionConfigService() {
    return UserAttributionConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  SensitiveDataConfigServiceBlockingStub provideSensitiveDataConfigService() {
    return SensitiveDataConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  DataClassificationConfigServiceBlockingStub provideDataClassificationConfigService() {
    return DataClassificationConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  DataParsingConfigServiceBlockingStub provideDataParsingConfigService() {
    return DataParsingConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  DataObfuscationConfigServiceBlockingStub provideDataObfuscationConfigService() {
    return DataObfuscationConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  InsightsServiceBlockingStub providesInsightsService() {
    return InsightsServiceGrpc.newBlockingStub(
            channelRegistry.forAddress(
                this.config.getConfig(INSIGHTS_SERVICE_CONFIG).getString("host"),
                this.config.getConfig(INSIGHTS_SERVICE_CONFIG).getInt("port")))
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  ClientConfig providesClientConfig() {
    return ClientConfig.DEFAULT;
  }
}
