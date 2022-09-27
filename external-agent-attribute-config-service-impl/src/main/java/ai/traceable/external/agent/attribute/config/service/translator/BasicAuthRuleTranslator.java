package ai.traceable.external.agent.attribute.config.service.translator;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.AUTH_HEADER_KEYS;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.BASIC_AUTH_TYPE;

import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.DataCase;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(onConstructor_ = {@Inject})
public class BasicAuthRuleTranslator implements RuleTranslator {

  private final AttributeKeysExtractor attributeKeysExtractor;
  private final AttributeRuleBuilder attributeRuleBuilder;

  @Override
  public DataCase getRuleDataCase() {
    return DataCase.BASIC_AUTHENTICATION_DATA;
  }

  @Override
  public Stream<AttributeRule> translateRuleForUserId(UserAttributionRule rule) {
    return attributeKeysExtractor
        .getHeaderAttributeKeysIfSet(rule.getData().getBasicAuthenticationData().getLocation())
        .orElse(AUTH_HEADER_KEYS)
        .stream()
        .map(key -> translateRuleForUserId(key, rule.getId()));
  }

  @Override
  public Stream<AttributeRule> translateRuleForAuthType(UserAttributionRule rule) {
    return attributeKeysExtractor
        .getHeaderAttributeKeysIfSet(rule.getData().getBasicAuthenticationData().getLocation())
        .orElse(AUTH_HEADER_KEYS)
        .stream()
        .map(key -> translateRuleForAuthType(key, rule.getId()));
  }

  private AttributeRule translateRuleForUserId(String key, String ruleId) {
    return getRule(key, attributeRuleBuilder.buildActionAttributeRuleForUserId(ruleId));
  }

  private AttributeRule translateRuleForAuthType(String key, String ruleId) {
    return getRule(
        key, attributeRuleBuilder.buildActionAttributeRuleForAuthType(BASIC_AUTH_TYPE, ruleId));
  }

  private AttributeRule getRule(String key, AttributeRule actionAttributeRule) {
    return attributeRuleBuilder.buildRuleForAttribute(
        key,
        attributeRuleBuilder.buildRuleForRegexCaptureGroup(
            "(?i)Basic:? (.*)",
            attributeRuleBuilder.buildRuleForBase64(
                attributeRuleBuilder.buildRuleForRegexCaptureGroup("(.*):", actionAttributeRule))));
  }
}
