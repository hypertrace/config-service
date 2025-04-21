package ai.traceable.genai.config.service.v1;

import ai.traceable.genai.config.service.v1.feature.config.GenAiFeatureConfigHandlerModule;
import ai.traceable.genai.config.service.v1.genai.config.DefaultGenAiConfigProvider;
import ai.traceable.genai.config.service.v1.manager.GenAiConfigManager;
import ai.traceable.genai.config.service.v1.manager.GenAiConfigManagerImpl;
import ai.traceable.genai.config.service.v1.store.GenAiConfigStore;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import lombok.AllArgsConstructor;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

@AllArgsConstructor
public class GenAiConfigServiceModule extends AbstractModule {

  private static final String GEN_AI_CONFIG_SERVICE_CONFIG_PATH = "gen.ai.config.service.config";

  private final Channel channel;
  private final Config config;
  private final ConfigChangeEventGenerator changeEventGenerator;

  @Override
  protected void configure() {
    bind(BindableService.class).to(GenAiConfigServiceImpl.class);
    bind(Config.class).toInstance(config);
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
    bind(Channel.class).toInstance(channel);
    bind(new TypeLiteral<IdentifiedObjectStore<GenAiConfig>>() {}).to(GenAiConfigStore.class);
    bind(GenAiConfig.class).toProvider(DefaultGenAiConfigProvider.class);
    bind(GenAiConfigManager.class).to(GenAiConfigManagerImpl.class);
    install(new GenAiFeatureConfigHandlerModule());
  }

  @Provides
  @Singleton
  GenAiConfigServiceConfig providesGenAiConfigServiceConfig() {
    return new GenAiConfigServiceConfig(config.getConfig(GEN_AI_CONFIG_SERVICE_CONFIG_PATH));
  }

  @Provides
  @Singleton
  ConfigServiceGrpc.ConfigServiceBlockingStub providesConfigService() {
    return ConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
