package ai.traceable.iprange.config.service.rules;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class RulesManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(RulesManager.class).to(IpRangeRulesManager.class);
    bind(RulesValidator.class).to(IpRangeRulesValidator.class);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub providesConfigService(ManagedChannel channel) {
    return ConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
