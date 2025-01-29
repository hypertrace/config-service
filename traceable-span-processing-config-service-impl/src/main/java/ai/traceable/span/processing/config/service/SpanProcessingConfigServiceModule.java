package ai.traceable.span.processing.config.service;

import ai.traceable.span.processing.config.service.apinamingrules.ApiNamingRulesManagerModule;
import ai.traceable.span.processing.config.service.protectionspanrules.ProtectionSpanRulesManagerModule;
import ai.traceable.span.processing.config.service.samplingconfigs.SamplingConfigManagerModule;
import ai.traceable.span.processing.config.service.servicenaming.ServiceNamingRuleModule;
import ai.traceable.span.processing.config.service.spaningestionrules.SpanIngestionRulesManagerModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Clock;
import java.time.Duration;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class SpanProcessingConfigServiceModule extends AbstractModule {
  private static final String SPAN_PROCESSING_CONFIG_SERVICE_CONFIG_PATH =
      "span.processing.config.service";
  private static final String HYPERTRACE_CONFIG_SERVICE_TIMEOUT_CONFIG_PATH =
      "hypertrace.config.service.timeout";

  private final Channel channel;
  private final Config config;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  public SpanProcessingConfigServiceModule(
      Channel channel, Config config, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.config = config;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(SpanProcessingConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(ConfigChangeEventGenerator.class).toInstance(this.configChangeEventGenerator);
    bind(Clock.class).toInstance(Clock.systemUTC());

    install(new SamplingConfigManagerModule());
    install(
        new ApiNamingRulesManagerModule(
            config.getConfig(SPAN_PROCESSING_CONFIG_SERVICE_CONFIG_PATH)));
    install(new ProtectionSpanRulesManagerModule());
    install(new ServiceNamingRuleModule());
    install(new SpanIngestionRulesManagerModule(config));
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

  @Singleton
  @Provides
  ClientConfig providesClientConfig() {
    return new ClientConfig(
        this.config.hasPath(HYPERTRACE_CONFIG_SERVICE_TIMEOUT_CONFIG_PATH)
            ? this.config.getDuration(HYPERTRACE_CONFIG_SERVICE_TIMEOUT_CONFIG_PATH)
            : Duration.ofSeconds(10));
  }
}
