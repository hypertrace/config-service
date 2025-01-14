package ai.traceable.edge.decision.config.service.aggregator.attributes;

import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface VariableEnricherBase {
  VariableDerivationMapping getVariable(RequestContext requestContext);
}
