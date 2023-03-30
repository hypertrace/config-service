package ai.traceable.jwt.extraction.config.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class JwtExtractionConfigServiceModule extends AbstractModule {
  private static final String JWT_EXTRACTION_CONFIG_KEY_PATH = "jwt.extraction.config.service";
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;
  private final Config config;

  JwtExtractionConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator, Config config) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(JwtExtractionConfigServiceImpl.class);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(DefaultJwtExtractionRuleConfig.class)
        .toInstance(
            new DefaultJwtExtractionRuleConfig(config.getConfig(JWT_EXTRACTION_CONFIG_KEY_PATH)));
  }

  @Provides
  ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
