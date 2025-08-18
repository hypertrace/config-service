package ai.traceable.cloud.edge.deployment.config.service.v1;

import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfigServiceGrpc;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfigServiceGrpc.CloudBotDeploymentConfigServiceBlockingStub;
import ai.traceable.cloud.edge.deployment.config.service.v1.manager.CloudEdgeDeploymentConfigManager;
import ai.traceable.cloud.edge.deployment.config.service.v1.manager.CloudEdgeDeploymentConfigManagerImpl;
import ai.traceable.cloud.edge.deployment.config.service.v1.shared.config.SharedConfigMetadataRegistry;
import ai.traceable.cloud.edge.deployment.config.service.v1.shared.config.SharedConfigMetadataRegistryImpl;
import ai.traceable.cloud.edge.deployment.config.service.v1.state.transitions.StateTransitionsRegistry;
import ai.traceable.cloud.edge.deployment.config.service.v1.state.transitions.StateTransitionsRegistryImpl;
import ai.traceable.cloud.edge.deployment.config.service.v1.validator.EdgeDeploymentUsageValidator;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.BindableService;
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
    bind(BindableService.class).to(CloudEdgeDeploymentConfigServiceImpl.class);
    bind(CloudEdgeDeploymentConfigManager.class).to(CloudEdgeDeploymentConfigManagerImpl.class);
    bind(SharedConfigMetadataRegistry.class).to(SharedConfigMetadataRegistryImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(StateTransitionsRegistry.class).to(StateTransitionsRegistryImpl.class);
    bind(EdgeDeploymentUsageValidator.class);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  CloudBotDeploymentConfigServiceBlockingStub provideCloudBotDeploymentConfigStub() {
    return CloudBotDeploymentConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
