package ai.traceable.localprocessing.config.service.apinaming.http;

import ai.traceable.anomaly.config.service.v1.trainer.TrainerConfigServiceGrpc;
import ai.traceable.localprocessing.config.service.apinaming.http.namingconfig.HttpApiNamingConfigManagerModule;
import ai.traceable.localprocessing.config.service.apinaming.http.trie.HttpApiNamingTrieManagerModule;
import ai.traceable.localprocessing.config.service.config.EntityQueryServiceConfig;
import ai.traceable.localprocessing.config.service.config.http.HttpApiNamingConfig;
import ai.traceable.platform.apientity.http.client.RegexPatternCachingClient;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class HttpApiNamingManagerModule extends AbstractModule {
  private final Config config;

  public HttpApiNamingManagerModule(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(HttpApiNamingManager.class).to(DefaultHttpApiNamingManager.class);
    install(new HttpApiNamingConfigManagerModule());
    install(new HttpApiNamingTrieManagerModule(this.config));
  }

  @Provides
  TrainerConfigServiceGrpc.TrainerConfigServiceBlockingStub
      providesTrainerConfigServiceBlockingStub(Channel channel) {
    return TrainerConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  EntityQueryServiceConfig providesEntityDataServiceConfig() {
    return new EntityQueryServiceConfig(this.config);
  }

  @Provides
  HttpApiNamingConfig providesApiNamingConfig() {
    return new HttpApiNamingConfig(this.config);
  }

  @Provides
  RegexPatternCachingClient providesRegexPatternCachingClient() {
    return new RegexPatternCachingClient(this.config);
  }
}
