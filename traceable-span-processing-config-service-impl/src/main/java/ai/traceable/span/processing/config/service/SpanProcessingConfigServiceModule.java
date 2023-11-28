package ai.traceable.span.processing.config.service;

import ai.traceable.span.processing.config.service.apinamingrules.ApiNamingRulesManagerModule;
import ai.traceable.span.processing.config.service.protectionspanrules.ProtectionSpanRulesManagerModule;
import ai.traceable.span.processing.config.service.samplingconfigs.SamplingConfigManagerModule;
import ai.traceable.span.processing.config.service.servicenaming.ServiceNamingRuleModule;
import ai.traceable.span.processing.config.service.spaningestionrules.SpanIngestionRulesManagerModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Clock;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class SpanProcessingConfigServiceModule extends AbstractModule {

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
    install(new ApiNamingRulesManagerModule());
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
}
