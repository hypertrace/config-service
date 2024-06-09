package ai.traceable.data.classification.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class DataClassificationConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;
  private final DataClassificationConfig config;
  private final FeatureCachingClient featureCachingClient;

  DataClassificationConfigServiceModule(
      Channel channel,
      ConfigChangeEventGenerator configChangeEventGenerator,
      Config config,
      FeatureCachingClient featureCachingClient) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.config = new DataClassificationConfig(config);
    this.featureCachingClient = featureCachingClient;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(DataClassificationConfigServiceImpl.class);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(FeatureCachingClient.class).toInstance(this.featureCachingClient);
    bind(DataClassificationConfig.class).toInstance(this.config);
  }

  @Provides
  ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
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
  DataClassificationConfigServiceBlockingStub provideOwnStub() {
    return DataClassificationConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  ClientConfig provideClientConfig() {
    return ClientConfig.DEFAULT;
  }
}
