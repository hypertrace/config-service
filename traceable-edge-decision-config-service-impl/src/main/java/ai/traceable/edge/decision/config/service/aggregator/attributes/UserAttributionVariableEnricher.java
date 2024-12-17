package ai.traceable.edge.decision.config.service.aggregator.attributes;

import static ai.traceable.edge.decision.config.service.VariableConstants.USER_ATTRIBUTION_VARIABLE_NAME;

import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.UserAttributionFetcher;
import com.google.inject.Inject;

public class UserAttributionVariableEnricher implements VariableEnricherBase {
  private final UserAttributionFetcher userAttributionFetcher;

  @Inject
  public UserAttributionVariableEnricher(UserAttributionFetcher userAttributionFetcher) {
    this.userAttributionFetcher = userAttributionFetcher;
  }

  public VariableDerivationMapping getVariable(String tenantId) {
    return VariableDerivationMapping.newBuilder()
        .setName(USER_ATTRIBUTION_VARIABLE_NAME.getValue())
        .addAllRules(userAttributionFetcher.getUserAttributionRules(tenantId))
        .build();
  }
}
