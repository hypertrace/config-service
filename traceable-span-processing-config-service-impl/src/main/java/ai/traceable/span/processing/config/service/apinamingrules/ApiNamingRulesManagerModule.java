package ai.traceable.span.processing.config.service.apinamingrules;

import ai.traceable.api.spec.config.service.v1.ApiSpecConfigServiceGrpc;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class ApiNamingRulesManagerModule extends AbstractModule {
  private final Config config;

  public ApiNamingRulesManagerModule(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(ApiNamingRulesManager.class).to(DefaultApiNamingRulesManager.class);
  }

  @Provides
  ApiSpecConfigServiceGrpc.ApiSpecConfigServiceBlockingStub
      providesSpanProcessingConfigServiceBlockingStub(Channel channel) {
    return ApiSpecConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  @Singleton
  ApiNamingRulesManagerConfig providesApiNamingRulesManagerConfig() {
    return new ApiNamingRulesManagerConfig(config);
  }
}
