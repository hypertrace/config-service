package ai.traceable.edge.decision.config.service.aggregator.attributes.variable.enrich.enricher;

import ai.traceable.edge.decision.config.service.VariableConstants;
import ai.traceable.edge.decision.config.service.aggregator.attributes.variable.enrich.CheckAndAddVariableToRule;
import ai.traceable.edge.decision.config.service.aggregator.attributes.variable.enrich.fetcher.VariableFetcher;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import com.google.inject.Inject;
import java.util.Map;
import java.util.Map.Entry;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@RequiredArgsConstructor(onConstructor_ = @Inject)
public class VariableConstantRuleEnricher implements VariableEnricher {

  private final CheckAndAddVariableToRule variableChecker;
  private final Map<VariableConstants, VariableFetcher> variableFetcherMap;

  @Override
  public EdgeDecisionEngineConfig enrichRule(
      RequestContext requestContext, EdgeDecisionEngineConfig edgeDecisionEngineConfig) {
    for (Entry<VariableConstants, VariableFetcher> enricherEntry : variableFetcherMap.entrySet()) {
      edgeDecisionEngineConfig =
          variableChecker.checkAndAddVariableToRule(
              edgeDecisionEngineConfig,
              enricherEntry.getKey().getValue(),
              enricherEntry.getValue().getVariable(requestContext));
    }
    return edgeDecisionEngineConfig;
  }
}
