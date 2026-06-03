package ai.traceable.waf.provider.integration.service;

import ai.traceable.job.service.v1.JobServiceGrpc;
import ai.traceable.job.service.v1.JobServiceGrpc.JobServiceBlockingStub;
import ai.traceable.waf.provider.integration.service.sync.JobServiceClientConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class WafIntegrationConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;
  private final Config config;
  private final GrpcChannelRegistry grpcChannelRegistry;

  WafIntegrationConfigServiceModule(
      Config config,
      Channel channel,
      ConfigChangeEventGenerator configChangeEventGenerator,
      GrpcChannelRegistry grpcChannelRegistry) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.config = config;
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(WafIntegrationConfigServiceImpl.class);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(Config.class).toInstance(config);
  }

  @Provides
  ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  @Singleton
  JobServiceClientConfig provideJobServiceClientConfig() {
    return JobServiceClientConfig.from(config);
  }

  @Provides
  @Singleton
  JobServiceBlockingStub provideJobServiceBlockingStub(final JobServiceClientConfig clientConfig) {
    return JobServiceGrpc.newBlockingStub(
            grpcChannelRegistry.forPlaintextAddress(clientConfig.getHost(), clientConfig.getPort()))
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
