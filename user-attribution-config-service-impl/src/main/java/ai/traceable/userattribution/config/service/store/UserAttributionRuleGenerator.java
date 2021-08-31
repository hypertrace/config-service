package ai.traceable.userattribution.config.service.store;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.userattribution.config.service.v1.CreateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import javax.inject.Inject;

public class UserAttributionRuleGenerator {

  private final UuidGenerator uuidGenerator;

  @Inject
  public UserAttributionRuleGenerator(UuidGenerator uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
  }

  public UserAttributionRule generateNewRuleWithoutRank(CreateUserAttributionRuleRequest request) {
    return UserAttributionRule.newBuilder()
        .setData(request.getData())
        .setName(request.getName())
        .setId(this.uuidGenerator.generateRandomId())
        .build();
  }
}
