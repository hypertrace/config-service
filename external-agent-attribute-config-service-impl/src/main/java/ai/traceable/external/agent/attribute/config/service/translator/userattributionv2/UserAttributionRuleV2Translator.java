package ai.traceable.external.agent.attribute.config.service.translator.userattributionv2;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import java.util.Map;
import java.util.stream.Stream;

public interface UserAttributionRuleV2Translator {

  default Stream<AttributeRule> translateRuleForUserId(UserAttributionRule rule) {
    return Stream.empty();
  }

  default Stream<AttributeRule> translateRuleForUserRole(UserAttributionRule rule) {
    return Stream.empty();
  }

  default Stream<AttributeRule> translateRuleForUserScope(UserAttributionRule rule) {
    return Stream.empty();
  }

  default Stream<AttributeRule> translateRuleForAuthType(UserAttributionRule rule) {
    return Stream.empty();
  }

  default Stream<Map.Entry<String, AttributeRule>> translateRuleForCustomTokens(
      UserAttributionRule rule) {
    return Stream.empty();
  }
}
