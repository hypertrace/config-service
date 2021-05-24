package ai.traceable.external.userattribution.config.service;

import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRules;
import ai.traceable.external.userattribution.config.service.v1.GetExternalUserAttributionRulesRequest;
import ai.traceable.external.userattribution.config.service.v1.GetExternalUserAttributionRulesResponse;
import javax.inject.Inject;

class ExternalUserAttributionRuleResponseBuilder {

  private final HashGenerator hashGenerator;

  @Inject
  ExternalUserAttributionRuleResponseBuilder(HashGenerator hashGenerator) {
    this.hashGenerator = hashGenerator;
  }

  GetExternalUserAttributionRulesResponse buildResponse(
      GetExternalUserAttributionRulesRequest request, ExternalUserAttributionRules rules) {
    String responseHash = this.hashGenerator.generateHash(rules);
    GetExternalUserAttributionRulesResponse.Builder responseBuilder =
        GetExternalUserAttributionRulesResponse.newBuilder().setHash(responseHash);

    if (!responseHash.equals(request.getFilter().getPreviousHash())) {
      responseBuilder.setExternalUserAttributionRules(rules);
    }

    return responseBuilder.build();
  }
}
