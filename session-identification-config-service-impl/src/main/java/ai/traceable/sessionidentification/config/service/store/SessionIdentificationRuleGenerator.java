package ai.traceable.sessionidentification.config.service.store;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.sessionidentification.config.service.v1.CreateSessionIdentificationRuleRequest;
import ai.traceable.sessionidentification.config.service.v1.RuleCreationSource;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRuleStatus;
import ai.traceable.sessionidentification.config.service.v1.UpdateSessionIdentificationRuleRequest;
import javax.inject.Inject;

public class SessionIdentificationRuleGenerator {
  private final UuidGenerator uuidGenerator;

  @Inject
  public SessionIdentificationRuleGenerator(UuidGenerator uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
  }

  public SessionIdentificationRule generateNewRuleFromCreateRequest(
      CreateSessionIdentificationRuleRequest request) {

    SessionIdentificationRule.Builder builder =
        SessionIdentificationRule.newBuilder()
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

  public SessionIdentificationRule generateRuleForOldApiConfigFromUpdateRequest(
      UpdateSessionIdentificationRuleRequest request) {
    return SessionIdentificationRule.newBuilder(
            generateRuleForOldApiConfigFromUpdateRequest(request))
        .setStatus(
            SessionIdentificationRuleStatus.newBuilder(request.getStatus())
                .setRuleCreationSource(RuleCreationSource.RULE_CREATION_SOURCE_OLD_API))
        .build();
  }

  public SessionIdentificationRule generateRuleFromUpdateRequest(
      UpdateSessionIdentificationRuleRequest request) {
    SessionIdentificationRule.Builder builder =
        SessionIdentificationRule.newBuilder()
            .setId(request.getId())
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
