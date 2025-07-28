package ai.traceable.cloud.bot.deployment.config.service.v1;

import ai.traceable.cloud.bot.deployment.config.service.v1.manager.CloudBotDeploymentConfigManager;
import ai.traceable.cloud.bot.deployment.config.service.v1.manager.CloudBotDeploymentConfigManagerImpl;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Clock;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class CloudBotDeploymentConfigServiceModule extends AbstractModule {

  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  CloudBotDeploymentConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(Clock.class).toInstance(Clock.systemUTC());
    bind(BindableService.class).to(CloudBotDeploymentConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(CloudBotDeploymentConfigManager.class).to(CloudBotDeploymentConfigManagerImpl.class);
    bind(UuidGenerator.class).toInstance(new UuidGenerator());
  }

  @Provides
  ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
