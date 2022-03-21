package ai.traceable.localprocessing.config.service.apinaming.http;

import ai.traceable.anomaly.config.service.v1.trainer.TrainerConfigServiceGrpc;
import ai.traceable.localprocessing.config.service.apinaming.http.namingconfig.HttpApiNamingConfigManagerModule;
import ai.traceable.localprocessing.config.service.apinaming.http.trie.HttpApiNamingTrieManagerModule;
import ai.traceable.localprocessing.config.service.config.EntityDataServiceConfig;
import ai.traceable.localprocessing.config.service.config.http.HttpApiNamingConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
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
      providesTrainerConfigServiceBlockingStub(ManagedChannel channel) {
    return TrainerConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  EntityDataServiceConfig providesEntityDataServiceConfig() {
    return new EntityDataServiceConfig(this.config);
  }

  @Provides
  HttpApiNamingConfig providesApiNamingConfig() {
    return new HttpApiNamingConfig(this.config);
  }
}
