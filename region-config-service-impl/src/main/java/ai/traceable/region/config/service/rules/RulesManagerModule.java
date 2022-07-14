package ai.traceable.region.config.service.rules;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.Channel;
import java.time.Clock;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class RulesManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(RulesManager.class).to(RegionRulesManager.class);
    bind(RulesValidator.class).to(RegionRulesValidator.class);
    bind(Clock.class).toInstance(Clock.systemUTC());
  }

  @Provides
  ConfigServiceBlockingStub providesConfigService(Channel channel) {
    return ConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
