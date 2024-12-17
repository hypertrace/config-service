package ai.traceable.edge.decision.config.service.aggregator.attributes;

import ai.traceable.edge.decision.config.service.VariableConstants;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import com.google.inject.Inject;
import java.util.Map;
import java.util.Map.Entry;

public class RuleVariableEnricher {
  private final CheckAndAddVariableToRule variableChecker;
  private final Map<VariableConstants, VariableEnricherBase> variableEnricherMap;

  @Inject
  public RuleVariableEnricher(
      CheckAndAddVariableToRule variableChecker,
      Map<VariableConstants, VariableEnricherBase> variableEnricherMap) {
    this.variableChecker = variableChecker;
    this.variableEnricherMap = variableEnricherMap;
  }

  public EdgeDecisionEngineConfig enrichRule(
      String tenantId, EdgeDecisionEngineConfig edgeDecisionEngineConfig) {
    for (Entry<VariableConstants, VariableEnricherBase> enricherEntry :
        variableEnricherMap.entrySet()) {
      edgeDecisionEngineConfig =
          variableChecker.checkAndAddVariableToRule(
              edgeDecisionEngineConfig,
              enricherEntry.getKey().getValue(),
              enricherEntry.getValue().getVariable(tenantId));
    }
    return edgeDecisionEngineConfig;
  }
}
