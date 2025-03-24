package ai.traceable.edge.decision.config.service.aggregator.attributes.variable.enrich;

import ai.traceable.edge.decision.config.service.aggregator.attributes.variable.enrich.enricher.VariableEnricher;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import com.google.inject.Inject;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@RequiredArgsConstructor(onConstructor_ = @Inject)
public class RuleVariableEnricher {
  private final Set<VariableEnricher> variableEnrichers;

  public EdgeDecisionEngineConfig enrichRule(
      RequestContext requestContext, EdgeDecisionEngineConfig edgeDecisionEngineConfig) {
    for (VariableEnricher enricher : variableEnrichers) {
      edgeDecisionEngineConfig = enricher.enrichRule(requestContext, edgeDecisionEngineConfig);
    }
    return edgeDecisionEngineConfig;
  }
}
