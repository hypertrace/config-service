package ai.traceable.edge.config.service.supplier.actor;

import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EdgeDecisionActorConfigSupplier {
  private final ActorDataCache actorDataCache;
  private final TraceableEdgeConfig config;

  @Inject
  public EdgeDecisionActorConfigSupplier(
      ActorDataCache actorDataCache, TraceableEdgeConfig config) {
    this.actorDataCache = actorDataCache;
    this.config = config;
  }

  public EdgeDecisionEngineConfig getEdgeDecisionActorConfig(RequestContext requestContext) {
    // TODO: Add the conversion logic
    return null;
  }
}
