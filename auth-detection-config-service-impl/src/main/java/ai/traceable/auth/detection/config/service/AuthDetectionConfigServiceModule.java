package ai.traceable.auth.detection.config.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class AuthDetectionConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;
  private final Config config;

  AuthDetectionConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator, Config config) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(AuthDetectionConfigServiceImpl.class);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(Config.class).toInstance(config);
  }

  @Provides
  ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
