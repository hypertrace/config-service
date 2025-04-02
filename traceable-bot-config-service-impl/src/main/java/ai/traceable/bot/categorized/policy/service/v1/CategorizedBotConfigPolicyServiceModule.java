package ai.traceable.bot.categorized.policy.service.v1;

import ai.traceable.entity.fetcher.cache.CachedServiceMappingProviderModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import lombok.RequiredArgsConstructor;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

@RequiredArgsConstructor
public class CategorizedBotConfigPolicyServiceModule extends AbstractModule {
  private static final String CACHED_SERVICE_MAPPING_NAME = "cachedServiceMapping-botService";

  private final Channel channel;
  private final ConfigChangeEventGenerator changeEventGenerator;
  private final GrpcChannelRegistry grpcChannelRegistry;
  private final Config config;

  @Override
  protected void configure() {
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
    bind(Config.class).toInstance(config);
    bind(BindableService.class).to(CategorizedBotConfigPolicyService.class);
    install(
        new CachedServiceMappingProviderModule(
            grpcChannelRegistry, config, CACHED_SERVICE_MAPPING_NAME));
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
