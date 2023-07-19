package ai.traceable.sessionidentification.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sessionidentification.config.service.migration.LegacySessionIdentificationRuleTranslatingDao;
import ai.traceable.sessionidentification.config.service.migration.LegacySessionIdentificationRuleTranslatingDaoImpl;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class SessionIdentificationConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;
  private final FeatureCachingClient featureCachingClient;
  private final Config config;

  SessionIdentificationConfigServiceModule(
      Channel channel,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureCachingClient,
      Config config) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.featureCachingClient = featureCachingClient;
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(SessionIdentificationConfigServiceImpl.class);
    bind(Config.class).toInstance(this.config);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(LegacySessionIdentificationRuleTranslatingDao.class)
        .to(LegacySessionIdentificationRuleTranslatingDaoImpl.class);
    bind(FeatureCachingClient.class).toInstance(this.featureCachingClient);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub
      providesSensitiveDataConfigService() {
    return SensitiveDataConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
