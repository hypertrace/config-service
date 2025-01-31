package ai.traceable.userattribution.config.service.v2.store;

import static ai.traceable.userattribution.config.service.v2.Source.SOURCE_UNSPECIFIED;
import static ai.traceable.userattribution.config.service.v2.Source.SOURCE_USER;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.userattribution.config.service.v2.CreateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v2.UpdateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleData;
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
            .setData(addSourceIfAbsent(request.getData()))
            .setId(this.uuidGenerator.generateRandomId());
    return builder.build();
  }

  public UserAttributionRule generateUpdatedRule(
      UpdateUserAttributionRuleRequest request, UserAttributionRule existingRule) {
    UserAttributionRule.Builder builder =
        existingRule.toBuilder().setData(addSourceIfAbsent(request.getData()));
    return builder.build();
  }

  private UserAttributionRuleData addSourceIfAbsent(UserAttributionRuleData ruleData) {
    if (ruleData.getSource().equals(SOURCE_UNSPECIFIED)) {
      // Unless specified, source is user
      return ruleData.toBuilder().setSource(SOURCE_USER).build();
    }
    return ruleData;
  }
}
