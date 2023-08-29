package ai.traceable.ratelimiting.service.v2;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProviderModule;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesManager;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesValidator;
import ai.traceable.ratelimiting.service.v2.rules.RulesManager;
import ai.traceable.ratelimiting.service.v2.rules.RulesValidator;
import ai.traceable.ratelimiting.service.v2.rules.modsec.RateLimitingModsecRulesModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Clock;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class RateLimitingConfigServiceModule extends AbstractModule {
  private static final String CACHED_SERVICE_MAPPING_NAME = "cachedServiceMapping-blockingConfig";

  private final Channel channel;
  private final Config config;
  private final ActivityEventProducer activityEventProducer;
  private final ConfigChangeEventGenerator configChangeEventGenerator;
  private final GrpcChannelRegistry grpcChannelRegistry;

  public RateLimitingConfigServiceModule(
      Channel channel,
      Config config,
      ActivityEventProducer activityEventProducer,
      ConfigChangeEventGenerator configChangeEventGenerator,
      GrpcChannelRegistry grpcChannelRegistry) {
    this.channel = channel;
    this.config = config;
    this.activityEventProducer = activityEventProducer;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  public void configure() {
    bind(BindableService.class).to(RateLimitingConfigServiceImpl.class);
    bind(ActivityEventProducer.class).toInstance(activityEventProducer);
    bind(RateLimitingConfigServiceConfig.class)
        .toInstance(new RateLimitingConfigServiceConfig(config));
    bind(RulesManager.class).to(RateLimitingRulesManager.class);
    bind(RulesValidator.class).to(RateLimitingRulesValidator.class);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(Clock.class).toInstance(Clock.systemUTC());

    install(new RateLimitingModsecRulesModule(channel));
    install(
        new CachedServiceMappingProviderModule(
            grpcChannelRegistry, config, CACHED_SERVICE_MAPPING_NAME));
  }

  @Provides
  ConfigServiceBlockingStub providesConfigService() {
    return ConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
