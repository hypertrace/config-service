package ai.traceable.api.spec.config.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Duration;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class ApiSpecConfigServiceModule extends AbstractModule {
  private static final String API_SPEC_CONFIG_SERVICE_CONFIG_PATH = "api.spec.config";
  private static final String HYPERTRACE_CONFIG_SERVICE_TIMEOUT_CONFIG_PATH =
      "hypertrace.config.service.timeout";

  private final Channel channel;
  private final Config config;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  public ApiSpecConfigServiceModule(
      Channel channel, Config config, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.config = config;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(ApiSpecConfigServiceImpl.class);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
  }

  @Provides
  public Config providesConfig() {
    return this.config;
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  ApiSpecConfig provideApiSpecConfig() {
    return new ApiSpecConfig(this.config.getConfig(API_SPEC_CONFIG_SERVICE_CONFIG_PATH));
  }

  @Singleton
  @Provides
  ClientConfig providesClientConfig() {
    return new ClientConfig(
        this.config.hasPath(HYPERTRACE_CONFIG_SERVICE_TIMEOUT_CONFIG_PATH)
            ? this.config.getDuration(HYPERTRACE_CONFIG_SERVICE_TIMEOUT_CONFIG_PATH)
            : Duration.ofSeconds(10));
  }
}
