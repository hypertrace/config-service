package ai.traceable.cloud.edge.deployment.config.service.v1;

import ai.traceable.cloud.edge.deployment.config.service.v1.manager.CloudEdgeDeploymentConfigManager;
import ai.traceable.cloud.edge.deployment.config.service.v1.manager.CloudEdgeDeploymentConfigManagerImpl;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class CloudEdgeDeploymentConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  CloudEdgeDeploymentConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(CloudEdgeDeploymentConfigManager.class).to(CloudEdgeDeploymentConfigManagerImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
