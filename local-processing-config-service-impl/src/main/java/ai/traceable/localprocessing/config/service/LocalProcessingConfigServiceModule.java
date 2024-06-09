package ai.traceable.localprocessing.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.localprocessing.config.service.apinaming.http.HttpApiNamingManagerModule;
import ai.traceable.localprocessing.config.service.coordinator.ConfigServiceCoordinatorModule;
import ai.traceable.localprocessing.config.service.customsignature.CustomModsecDetectionManagerModule;
import ai.traceable.localprocessing.config.service.regularmodsec.RegularModsecDetectionManagerModule;
import ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class LocalProcessingConfigServiceModule extends AbstractModule {

  private final Channel channel;
  private final Config config;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  private final FeatureCachingClient featureCachingClient;

  public LocalProcessingConfigServiceModule(
      Channel channel,
      Config config,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureCachingClient) {
    this.channel = channel;
    this.config = config;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.featureCachingClient = featureCachingClient;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(LocalProcessingConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(FeatureCachingClient.class).toInstance(this.featureCachingClient);
    install(new ConfigServiceCoordinatorModule());
    install(new CustomModsecDetectionManagerModule());
    install(new RegularModsecDetectionManagerModule());
    install(new HttpApiNamingManagerModule(this.config));
    install(new SpanProcessingRulesManagerModule(this.config));
  }

  @Provides
  public Config providesConfig() {
    return this.config;
  }

  @Provides
  LocalProcessingConfigServiceConfig providesCustomSignatureServiceConfig() {
    return new LocalProcessingConfigServiceConfig(this.config);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  ClientConfig providesClientConfig() {
    return ClientConfig.DEFAULT;
  }
}
