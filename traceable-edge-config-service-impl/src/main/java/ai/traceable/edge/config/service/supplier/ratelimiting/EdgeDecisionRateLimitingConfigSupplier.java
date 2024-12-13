package ai.traceable.edge.config.service.supplier.ratelimiting;

import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingEdgeDecisionRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EdgeDecisionRateLimitingConfigSupplier {
  private final RateLimitingConfigServiceBlockingStub stub;
  private final TraceableEdgeConfig config;

  @Inject
  public EdgeDecisionRateLimitingConfigSupplier(
      TraceableEdgeConfig config, RateLimitingConfigServiceBlockingStub stub) {
    this.stub = stub;
    this.config = config;
  }

  public EdgeDecisionEngineConfig getEdgeDecisionRateLimitingActorConfig(
      RequestContext requestContext) {
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
                                .getRateLimitingEdgeDecisionRules(
                                    GetRateLimitingEdgeDecisionRulesRequest.getDefaultInstance()))
                    .getEdgeDecisionEngineConfig())
        .orElseGet(EdgeDecisionEngineConfig::getDefaultInstance);
  }
}
