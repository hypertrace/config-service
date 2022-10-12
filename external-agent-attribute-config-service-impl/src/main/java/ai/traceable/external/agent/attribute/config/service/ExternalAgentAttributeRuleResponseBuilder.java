package ai.traceable.external.agent.attribute.config.service;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.GetAgentAttributeRulesRequest;
import ai.traceable.external.agent.attribute.config.service.v1.GetAgentAttributeRulesResponse;
import java.util.List;
import javax.inject.Inject;

class ExternalAgentAttributeRuleResponseBuilder {

  private final UuidGenerator uuidGenerator;

  @Inject
  ExternalAgentAttributeRuleResponseBuilder(UuidGenerator uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
  }

  GetAgentAttributeRulesResponse buildResponse(
      GetAgentAttributeRulesRequest request, List<AttributeRule> rules) {
    String responseHash = this.uuidGenerator.generateId(rules);
    GetAgentAttributeRulesResponse.Builder responseBuilder =
        GetAgentAttributeRulesResponse.newBuilder().setHash(responseHash);

    if (!responseHash.equals(request.getFilter().getPreviousHash())) {
      responseBuilder.addAllAgentAttributeRules(rules);
    }

    return responseBuilder.build();
  }
}
