package ai.traceable.edge.config.service.supplier;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.bot.config.service.v1.BotConfigServiceGrpc;
import ai.traceable.edge.bot.config.service.v1.FlowConfig;
import ai.traceable.edge.bot.config.service.v1.GetAllFlowConfigsRequest;
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

public class FlowConfigSupplier implements TraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = FlowConfig.class.getSimpleName();
  private final BotConfigServiceGrpc.BotConfigServiceBlockingStub stub;
  private final TraceableEdgeConfig config;
  private final UuidGenerator uuidGenerator;

  @Inject
  public FlowConfigSupplier(
      TraceableEdgeConfig config,
      BotConfigServiceGrpc.BotConfigServiceBlockingStub stub,
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
      String environment,
      ConfigRequestElement requestElement,
      AgentCapabilities agentCapabilities) {
    var allCaptchaSiteKeyConfigs =
        requestContext.call(
            () ->
                stub.withDeadlineAfter(
                        config.getClientConfig().getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getAllFlowConfigs(GetAllFlowConfigsRequest.getDefaultInstance()));
    ConfigPayloads.Builder configPayloadsBuilder = ConfigPayloads.newBuilder();
    allCaptchaSiteKeyConfigs.getConfigsList().stream()
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
