package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher;

import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import java.util.List;

public interface UserAttributionFetcher {
  List<DerivationRule> getUserAttributionRules(String tenantId);
}
