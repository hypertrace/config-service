package ai.traceable.sessionattribution.config.service.store;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.sessionattribution.config.service.v1.CreateSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.v1.SessionAttributionRule;
import ai.traceable.sessionattribution.config.service.v1.UpdateSessionAttributionRuleRequest;
import javax.inject.Inject;

public class SessionAttributionRuleGenerator {
  private final UuidGenerator uuidGenerator;

  @Inject
  public SessionAttributionRuleGenerator(UuidGenerator uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
  }

  public SessionAttributionRule generateNewRuleWithoutRank(
      CreateSessionAttributionRuleRequest request) {

    SessionAttributionRule.Builder builder =
        SessionAttributionRule.newBuilder()
            .addAllTokenRules(request.getTokenRulesList())
            .setName(request.getName())
            .setId(this.uuidGenerator.generateRandomId());
    if (request.hasDescription()) {
      builder.setDescription(request.getDescription());
    }
    if (request.hasScope()) {
      builder.setScope(request.getScope());
    }
    return builder.build();
  }

  public SessionAttributionRule generateRuleFromUpdateRequest(
      UpdateSessionAttributionRuleRequest request, SessionAttributionRule existingRule) {
    SessionAttributionRule.Builder builder =
        SessionAttributionRule.newBuilder()
            .setId(request.getId())
            .setRank(existingRule.getRank())
            .addAllTokenRules(request.getTokenRulesList())
            .setName(request.getName());
    if (request.hasDescription()) {
      builder.setDescription(request.getDescription());
    }
    if (request.hasScope()) {
      builder.setScope(request.getScope());
    }
    if (request.hasStatus()) {
      builder.setStatus(request.getStatus());
    }
    return builder.build();
  }
}
