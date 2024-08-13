package ai.traceable.span.processing.config.service.apinamingrules;

import ai.traceable.api.spec.config.service.v1.ApiSpecConfigServiceGrpc;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class ApiNamingRulesManagerModule extends AbstractModule {

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
}
