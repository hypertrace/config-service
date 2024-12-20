package ai.traceable.userattribution.config.service.v1.store;

import static ai.traceable.userattribution.config.service.v1.store.UserAttributionRuleScopeUtils.setUserAttributionRuleScopeIfNotPresent;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.userattribution.config.service.v1.CreateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import jakarta.inject.Inject;

public class UserAttributionRuleGenerator {

  private final UuidGenerator uuidGenerator;

  @Inject
  public UserAttributionRuleGenerator(UuidGenerator uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
  }

  public UserAttributionRule generateNewRuleWithoutRank(CreateUserAttributionRuleRequest request) {
    UserAttributionRule.Builder builder =
        UserAttributionRule.newBuilder()
            .setData(request.getData())
            .setName(request.getName())
            .setId(this.uuidGenerator.generateRandomId());

    if (request.hasScope()) {
      builder.setScope(request.getScope());
    }

    setUserAttributionRuleScopeIfNotPresent(builder);

    return builder.build();
  }
}
