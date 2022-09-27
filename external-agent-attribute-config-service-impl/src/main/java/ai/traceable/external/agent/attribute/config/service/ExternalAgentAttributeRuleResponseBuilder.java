package ai.traceable.external.agent.attribute.config.service;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRules;
import ai.traceable.external.agent.attribute.config.service.v1.GetAgentAttributeRulesRequest;
import ai.traceable.external.agent.attribute.config.service.v1.GetAgentAttributeRulesResponse;
import javax.inject.Inject;

class ExternalAgentAttributeRuleResponseBuilder {

  private final UuidGenerator uuidGenerator;

  @Inject
  ExternalAgentAttributeRuleResponseBuilder(UuidGenerator uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
  }

  GetAgentAttributeRulesResponse buildResponse(
      GetAgentAttributeRulesRequest request, AgentAttributeRules rules) {
    String responseHash = this.uuidGenerator.generateId(rules);
    GetAgentAttributeRulesResponse.Builder responseBuilder =
        GetAgentAttributeRulesResponse.newBuilder().setHash(responseHash);

    if (!responseHash.equals(request.getFilter().getPreviousHash())) {
      responseBuilder.setAgentAttributeRules(rules);
    }

    return responseBuilder.build();
  }
}
