package ai.traceable.edge.config.service.supplier;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionConfigServiceGrpc;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionConfigsFilter;
import ai.traceable.edge.decision.config.service.v1.GetResolvedEdgeDecisionEngineConfigsRequest;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * This is a proxy layer, the business logic of constructing the config must stay in the respective
 * modules.
 */
public class EdgeDecisionEngineConfigSupplier implements TraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = EdgeDecisionEngineConfig.class.getSimpleName();
  private final EdgeDecisionConfigServiceGrpc.EdgeDecisionConfigServiceBlockingStub stub;
  private final TraceableEdgeConfig config;
  private final UuidGenerator uuidGenerator;

  @Inject
  public EdgeDecisionEngineConfigSupplier(
      TraceableEdgeConfig config,
      EdgeDecisionConfigServiceGrpc.EdgeDecisionConfigServiceBlockingStub stub,
      UuidGenerator uuidGenerator) {
    this.stub = stub;
    this.config = config;
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
    GetEdgeDecisionConfigsFilter.Builder filter = GetEdgeDecisionConfigsFilter.newBuilder();
    // todo: convert agent capabilities to input kind filter
    //    for (var component : agentCapabilities.getComponentsList()) {
    //      if (component.hasTraceableEdgeBotServiceVersion()) {
    //        filter.addEdgeInputKinds(EdgeInputKind.EDGE_INPUT_KIND_CLIENT_FINGERPRINT_DATA);
    //      }
    //    }
    EdgeDecisionEngineConfig edgeDecisionEngineConfig =
        getResolvedEdgeDecisionEngineConfig(requestContext, filter.build());
    ConfigPayloads configPayloads =
        ConfigPayloads.newBuilder().addConfigBytes(edgeDecisionEngineConfig.toByteString()).build();
    return ConfigResponseElement.newBuilder()
        .setConfigType(getConfigType())
        .setEnabled(true)
        .addSupportedAgentCapabilities(agentCapabilities)
        .setRefreshAfterDuration(config.getAgentPollingFrequency(getConfigType()))
        .setConfigPayloads(configPayloads)
        .setHash(uuidGenerator.generateId(configPayloads))
        .build();
  }

  private EdgeDecisionEngineConfig getResolvedEdgeDecisionEngineConfig(
      RequestContext requestContext, GetEdgeDecisionConfigsFilter filter) {
    Optional<String> tenantIdHolder = requestContext.getTenantId();
    return tenantIdHolder
        .map(
            s ->
                requestContext
                    .call(
                        () ->
                            stub.withDeadlineAfter(
                                    config.getClientConfig().getTimeout().toMillis(),
                                    TimeUnit.MILLISECONDS)
                                .getResolvedEdgeDecisionEngineConfigs(
                                    GetResolvedEdgeDecisionEngineConfigsRequest.newBuilder()
                                        .setFilter(filter)
                                        .build()))
                    .getEdgeDecisionEngineConfig())
        .orElseGet(EdgeDecisionEngineConfig::getDefaultInstance);
  }
}
