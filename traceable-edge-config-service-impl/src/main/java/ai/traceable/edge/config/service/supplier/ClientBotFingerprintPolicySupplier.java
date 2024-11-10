package ai.traceable.edge.config.service.supplier;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigObject;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.policy.config.service.v1.GetAllRequest;
import ai.traceable.policy.config.service.v1.TraceablePolicyConfigServiceGrpc;
import ai.traceable.policy.config.service.v1.TraceablePolicyList;
import com.google.protobuf.Duration;
import com.typesafe.config.Config;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ClientBotFingerprintPolicySupplier implements TraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = "ClientBotFingerprintPolicy";
  private final TraceablePolicyConfigServiceGrpc.TraceablePolicyConfigServiceBlockingStub stub;
  private final ClientConfig clientConfig;
  private final Duration agentPollingFrequency;
  private final UuidGenerator uuidGenerator;

  @Inject
  public ClientBotFingerprintPolicySupplier(
      Config config,
      TraceablePolicyConfigServiceGrpc.TraceablePolicyConfigServiceBlockingStub stub,
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
    var allPoliciesResponse =
        requestContext.call(
            () ->
                stub.withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getAll(GetAllRequest.getDefaultInstance()));
    TraceablePolicyList policyList =
        TraceablePolicyList.newBuilder()
            .addAllPolicies(allPoliciesResponse.getPoliciesList())
            .build();
    var configObject =
        ConfigObject.newBuilder()
            .setEnabled(true)
            .setConfigDeserializer(EdgeDecisionEngineConfig.class.getName())
            .setConfigType(requestElement.getConfigType())
            .setConfigJson(ProtoUtils.serialize(policyList))
            .build();
    return ConfigResponseElement.newBuilder()
        .setEnabled(true)
        .setRefreshAfterDuration(agentPollingFrequency)
        .setConfigObject(configObject)
        .setHash(uuidGenerator.generateId(configObject))
        .build();
  }
}
