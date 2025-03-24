package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher;

import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface UserAttributionRuleFetcher {
  List<DerivationRule> getUserAttributionRules(RequestContext requestContext);
}
