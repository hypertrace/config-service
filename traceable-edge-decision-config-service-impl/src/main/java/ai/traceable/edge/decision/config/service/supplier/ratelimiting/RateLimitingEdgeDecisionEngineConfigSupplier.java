package ai.traceable.edge.decision.config.service.supplier.ratelimiting;

import ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingEdgeDecisionRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub;
import java.util.Optional;
import javax.inject.Inject;
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
  public EdgeDecisionEngineConfig get(RequestContext requestContext) {
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
