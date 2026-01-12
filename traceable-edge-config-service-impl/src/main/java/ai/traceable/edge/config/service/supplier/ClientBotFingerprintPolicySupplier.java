package ai.traceable.edge.config.service.supplier;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.AbstractTraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.policy.config.service.v1.ClientBotFingerprintPolicy;
import ai.traceable.policy.config.service.v1.GetAllRequest;
import ai.traceable.policy.config.service.v1.TraceablePolicyConfigServiceGrpc;
import jakarta.inject.Inject;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ClientBotFingerprintPolicySupplier extends AbstractTraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = ClientBotFingerprintPolicy.class.getSimpleName();
  private final TraceablePolicyConfigServiceGrpc.TraceablePolicyConfigServiceBlockingStub stub;

  @Inject
  public ClientBotFingerprintPolicySupplier(
      TraceableEdgeConfig config,
      TraceablePolicyConfigServiceGrpc.TraceablePolicyConfigServiceBlockingStub stub,
      UuidGenerator uuidGenerator) {
    super(uuidGenerator, config);
    this.stub = stub;
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
    var allPoliciesResponse =
        requestContext.call(
            () ->
                stub.withDeadlineAfter(
                        config.getClientConfig().getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getAll(GetAllRequest.getDefaultInstance()));
    ConfigPayloads.Builder configPayloadsBuilder = ConfigPayloads.newBuilder();
    for (var policyConfig : allPoliciesResponse.getPoliciesList()) {
      configPayloadsBuilder.addConfigBytes(
          policyConfig.getClientBotFingerprintPolicy().toByteString());
    }
    ConfigPayloads configPayloads = configPayloadsBuilder.build();

    // Use the generic builder method which includes configType in hash
    return buildConfigResponseElement(configPayloads, agentCapabilities);
  }
}
