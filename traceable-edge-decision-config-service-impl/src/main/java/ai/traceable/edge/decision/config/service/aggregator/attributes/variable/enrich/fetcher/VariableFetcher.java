package ai.traceable.edge.decision.config.service.aggregator.attributes.variable.enrich.fetcher;

import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface VariableFetcher {
  VariableDerivationMapping getVariable(RequestContext requestContext);
}
