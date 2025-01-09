package ai.traceable.edge.decision.config.service.supplier.ratelimiting;

import ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeInputKind;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionConfigsFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingEdgeDecisionRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingEdgeDecisionEngineConfigSupplier
    implements EdgeDecisionEngineConfigSupplier {
  private final RateLimitingConfigServiceBlockingStub stub;

  @Inject
  public RateLimitingEdgeDecisionEngineConfigSupplier(RateLimitingConfigServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public String getName() {
    return RateLimitingEdgeDecisionEngineConfigSupplier.class.getSimpleName();
  }

  @Override
  public EdgeDecisionEngineConfig get(
      RequestContext requestContext, GetEdgeDecisionConfigsFilter filter) {
    if (filter != null) {
      List<EdgeInputKind> inputKinds = filter.getEdgeInputKindsList();
      if (!inputKinds.isEmpty()
          && !inputKinds.contains(EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST)) {
        // currently, this supplier doesn't provide any other input kind.
        return EdgeDecisionEngineConfig.getDefaultInstance();
      }
    }
    Optional<String> tenantIdHolder = requestContext.getTenantId();
    return tenantIdHolder
        .map(
            s ->
                requestContext
                    .call(
                        () ->
                            stub.getRateLimitingEdgeDecisionRules(
                                GetRateLimitingEdgeDecisionRulesRequest.getDefaultInstance()))
                    .getEdgeDecisionEngineConfig())
        .orElseGet(EdgeDecisionEngineConfig::getDefaultInstance);
  }
}
