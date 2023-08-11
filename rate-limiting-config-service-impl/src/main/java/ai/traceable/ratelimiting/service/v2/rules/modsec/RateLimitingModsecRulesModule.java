package ai.traceable.ratelimiting.service.v2.rules.modsec;

import ai.traceable.anomaly.config.service.registry.AnomalyConfigRegistryModule;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class RateLimitingModsecRulesModule extends AbstractModule {
  private final Channel channel;

  public RateLimitingModsecRulesModule(Channel channel) {
    this.channel = channel;
  }

  @Override
  protected void configure() {
    bind(Channel.class).toInstance(channel);
    install(new AnomalyConfigRegistryModule());
  }

  @Provides
  DataClassificationConfigServiceBlockingStub providesDataClassificationConfigServiceBlockingStub(
      Channel channel) {
    return DataClassificationConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
