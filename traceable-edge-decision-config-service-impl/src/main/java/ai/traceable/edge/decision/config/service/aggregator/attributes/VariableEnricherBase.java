package ai.traceable.edge.decision.config.service.aggregator.attributes;

import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;

public interface VariableEnricherBase {
  VariableDerivationMapping getVariable(String tenantId);
}
