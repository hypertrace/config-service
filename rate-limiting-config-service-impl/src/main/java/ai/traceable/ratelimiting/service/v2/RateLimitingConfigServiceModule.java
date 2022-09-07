package ai.traceable.ratelimiting.service.v2;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesManager;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesValidator;
import ai.traceable.ratelimiting.service.v2.rules.RulesManager;
import ai.traceable.ratelimiting.service.v2.rules.RulesValidator;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class RateLimitingConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final RateLimitingConfigServiceConfig config;
  private final ActivityEventProducer activityEventProducer;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  RateLimitingConfigServiceModule(
      Channel channel,
      Config config,
      ActivityEventProducer activityEventProducer,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.config = new RateLimitingConfigServiceConfig(config);
    this.activityEventProducer = activityEventProducer;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  public void configure() {
    bind(BindableService.class).to(RateLimitingConfigServiceImpl.class);
    bind(ActivityEventProducer.class).toInstance(activityEventProducer);
    bind(RateLimitingConfigServiceConfig.class).toInstance(config);
    bind(RulesManager.class).to(RateLimitingRulesManager.class);
    bind(RulesValidator.class).to(RateLimitingRulesValidator.class);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
  }

  @Provides
  ConfigServiceBlockingStub providesConfigService() {
    return ConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
