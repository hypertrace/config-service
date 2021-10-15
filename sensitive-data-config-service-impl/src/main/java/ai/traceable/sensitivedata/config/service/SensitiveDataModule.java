package ai.traceable.sensitivedata.config.service;

import ai.traceable.platform.insights.api.v1.InsightsServiceGrpc;
import ai.traceable.platform.insights.api.v1.InsightsServiceGrpc.InsightsServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class SensitiveDataModule extends AbstractModule {

  private final Channel configChannel;
  private final Config config;
  private final GrpcChannelRegistry channelRegistry;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  SensitiveDataModule(
      Channel configChannel,
      Config config,
      GrpcChannelRegistry channelRegistry,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    this.configChannel = configChannel;
    this.config = config;
    this.channelRegistry = channelRegistry;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(Config.class).toInstance(this.config);
    bind(GrpcChannelRegistry.class).toInstance(this.channelRegistry);
    bind(ConfigServiceCoordinator.class).to(ConfigServiceCoordinatorImpl.class);
    bind(InsightsServiceCoordinator.class).to(InsightsServiceCoordinatorImpl.class);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
  }

  @Provides
  ConfigServiceBlockingStub providesConfigService() {
    return ConfigServiceGrpc.newBlockingStub(this.configChannel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  InsightsServiceBlockingStub providesInsightsService(SensitiveDataServiceConfig config) {
    return InsightsServiceGrpc.newBlockingStub(config.insightsChannel())
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
