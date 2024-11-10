package ai.traceable.edge.config.service.supplier;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigObject;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionConfigServiceGrpc;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigRequest;
import com.google.protobuf.Duration;
import com.typesafe.config.Config;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EdgeDecisionEngineConfigSupplier implements TraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = "EdgeDecisionEngineConfig";
  private final EdgeDecisionConfigServiceGrpc.EdgeDecisionConfigServiceBlockingStub stub;
  private final ClientConfig clientConfig;
  private final Duration agentPollingFrequency;
  private final UuidGenerator uuidGenerator;

  @Inject
  public EdgeDecisionEngineConfigSupplier(
      Config config,
      EdgeDecisionConfigServiceGrpc.EdgeDecisionConfigServiceBlockingStub stub,
      ClientConfig clientConfig,
      UuidGenerator uuidGenerator) {
    this.stub = stub;
    this.clientConfig = clientConfig;
    this.uuidGenerator = uuidGenerator;
    this.agentPollingFrequency = getAgentPollingFrequency(config, CONFIG_TYPE);
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
    var edgeDecisionEngineConfig =
        requestContext.call(
            () ->
                stub.withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getEdgeDecisionEngineConfig(
                        GetEdgeDecisionEngineConfigRequest.getDefaultInstance()));
    var configObject =
        ConfigObject.newBuilder()
            .setEnabled(true)
            .setConfigDeserializer(EdgeDecisionEngineConfig.class.getName())
            .setConfigType(requestElement.getConfigType())
            .setConfigJson(
                ProtoUtils.serialize(edgeDecisionEngineConfig.getEdgeDecisionEngineConfig()))
            .build();
    return ConfigResponseElement.newBuilder()
        .setEnabled(true)
        .setRefreshAfterDuration(agentPollingFrequency)
        .setConfigObject(configObject)
        .setHash(uuidGenerator.generateId(configObject))
        .build();
  }
}
