package ai.traceable.external.agent.attribute.config.service.translator.userattribution;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import java.util.stream.Stream;

public interface UserAttributionRuleTranslator {

  UserAttributionRuleData.DataCase getRuleDataCase();

  default Stream<AttributeRule> translateRuleForUserId(UserAttributionRule rule) {
    return Stream.empty();
  }

  default Stream<AttributeRule> translateRuleForUserRole(UserAttributionRule rule) {
    return Stream.empty();
  }

  default Stream<AttributeRule> translateRuleForAuthType(UserAttributionRule rule) {
    return Stream.empty();
  }
}
