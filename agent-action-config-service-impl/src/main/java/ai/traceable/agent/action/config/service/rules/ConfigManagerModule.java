package ai.traceable.agent.action.config.service.rules;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.Channel;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class ConfigManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ConfigManager.class).to(ConfigManagerImpl.class);
  }

  @Provides
  ConfigServiceBlockingStub providesConfigService(Channel channel) {
    return ConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
