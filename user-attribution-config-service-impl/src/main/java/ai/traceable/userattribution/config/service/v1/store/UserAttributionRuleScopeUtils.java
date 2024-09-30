package ai.traceable.userattribution.config.service.v1.store;

import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope;

class UserAttributionRuleScopeUtils {

  static void setUserAttributionRuleScopeIfNotPresent(UserAttributionRule.Builder builder) {
    if (!builder.hasScope()) {
      // setting scope as system wide by default
      builder.setScope(
          UserAttributionRuleScope.newBuilder()
              .setSystemWideScope(UserAttributionRuleScope.SystemWideScope.getDefaultInstance())
              .build());
    }
  }
}
