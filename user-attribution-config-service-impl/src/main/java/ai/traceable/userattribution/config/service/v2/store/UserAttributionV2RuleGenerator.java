package ai.traceable.userattribution.config.service.v2.store;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.userattribution.config.service.v2.CreateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v2.UpdateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import jakarta.inject.Inject;

public class UserAttributionV2RuleGenerator {

  private final UuidGenerator uuidGenerator;

  @Inject
  UserAttributionV2RuleGenerator(UuidGenerator uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
  }

  public UserAttributionRule generateNewRuleWithoutRank(CreateUserAttributionRuleRequest request) {
    UserAttributionRule.Builder builder =
        UserAttributionRule.newBuilder()
            .setData(request.getData())
            .setId(this.uuidGenerator.generateRandomId());
    return builder.build();
  }

  public UserAttributionRule generateUpdatedRule(
      UpdateUserAttributionRuleRequest request, UserAttributionRule existingRule) {
    UserAttributionRule.Builder builder = existingRule.toBuilder().setData(request.getData());
    return builder.build();
  }
}
