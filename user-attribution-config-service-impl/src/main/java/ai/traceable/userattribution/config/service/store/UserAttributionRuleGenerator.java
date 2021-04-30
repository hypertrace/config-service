package ai.traceable.userattribution.config.service.store;

import ai.traceable.userattribution.config.service.v1.CreateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import java.util.UUID;

public class UserAttributionRuleGenerator {
  public UserAttributionRule generateNewRule(CreateUserAttributionRuleRequest request) {
    return UserAttributionRule.newBuilder()
        .setData(request.getData())
        .setName(request.getName())
        .setId(this.generateNewId())
        .build();
  }

  private String generateNewId() {
    return UUID.randomUUID().toString();
  }
}
