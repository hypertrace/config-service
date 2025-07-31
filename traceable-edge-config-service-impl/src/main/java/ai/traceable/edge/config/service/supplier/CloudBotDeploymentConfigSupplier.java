package ai.traceable.edge.config.service.supplier;

import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfigServiceGrpc.CloudBotDeploymentConfigServiceBlockingStub;
import ai.traceable.cloud.bot.deployment.config.service.v1.GetCloudBotDeploymentConfigsRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.GetCloudBotDeploymentConfigsResponse;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import com.google.protobuf.AbstractMessageLite;
import jakarta.inject.Inject;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class CloudBotDeploymentConfigSupplier implements TraceableEdgeConfigSupplier {

  private static final String CONFIG_TYPE = CloudBotDeploymentConfig.class.getSimpleName();
  private final CloudBotDeploymentConfigServiceBlockingStub stub;
  private final TraceableEdgeConfig config;
  private final UuidGenerator uuidGenerator;

  @Inject
  public CloudBotDeploymentConfigSupplier(
      TraceableEdgeConfig config,
      CloudBotDeploymentConfigServiceBlockingStub stub,
      UuidGenerator uuidGenerator) {
    this.config = config;
    this.stub = stub;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public String getConfigType() {
    return CONFIG_TYPE;
  }

  @Override
  public ConfigResponseElement getConfigs(
      RequestContext requestContext,
      ConfigRequestElement requestElement,
      AgentCapabilities agentCapabilities) {
    GetCloudBotDeploymentConfigsResponse botDeploymentConfigs =
        requestContext.call(
            () ->
                stub.withDeadlineAfter(
                        config.getClientConfig().getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getCloudBotDeploymentConfigs(
                        GetCloudBotDeploymentConfigsRequest.getDefaultInstance()));
    ConfigPayloads.Builder configPayloadsBuilder = ConfigPayloads.newBuilder();
    botDeploymentConfigs.getCloudBotDeploymentsList().stream()
        .map(AbstractMessageLite::toByteString)
        .forEach(configPayloadsBuilder::addConfigBytes);
    ConfigPayloads configPayloads = configPayloadsBuilder.build();
    return ConfigResponseElement.newBuilder()
        .setConfigType(getConfigType())
        .setEnabled(true)
        .addSupportedAgentCapabilities(agentCapabilities)
        .setRefreshAfterDuration(config.getAgentPollingFrequency(getConfigType()))
        .setConfigPayloads(configPayloads)
        .setHash(uuidGenerator.generateId(configPayloads))
        .build();
  }
}
