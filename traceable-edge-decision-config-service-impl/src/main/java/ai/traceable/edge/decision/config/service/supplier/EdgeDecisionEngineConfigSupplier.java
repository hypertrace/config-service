package ai.traceable.edge.decision.config.service.supplier;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

/** Each supplier must provide the rules, specs and common variables. */
public interface EdgeDecisionEngineConfigSupplier {
  String getName();

  EdgeDecisionEngineConfig get(RequestContext requestContext);
}
