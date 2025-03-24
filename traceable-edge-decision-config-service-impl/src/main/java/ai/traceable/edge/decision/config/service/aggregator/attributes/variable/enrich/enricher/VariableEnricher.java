package ai.traceable.edge.decision.config.service.aggregator.attributes.variable.enrich.enricher;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface VariableEnricher {

  EdgeDecisionEngineConfig enrichRule(
      RequestContext requestContext, EdgeDecisionEngineConfig edgeDecisionEngineConfig);
}
