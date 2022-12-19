package ai.traceable.external.agent.attribute.config.service.translator.userattribution;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.AUTH_HEADER_KEYS;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.DataCase;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.HeaderLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.ParsingTarget;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(onConstructor_ = {@Inject})
public class BasicAuthRuleTranslator implements UserAttributionRuleTranslator {

  private final AttributeKeysExtractor attributeKeysExtractor;
  private final AttributeRuleBuilder attributeRuleBuilder;

  @Override
  public DataCase getRuleDataCase() {
    return DataCase.BASIC_AUTHENTICATION_DATA;
  }

  @Override
  public Stream<AttributeRule> translateRuleForUserId(UserAttributionRule rule) {
    HeaderLocation headerLocation = rule.getData().getBasicAuthenticationData().getLocation();
    return attributeKeysExtractor
        .getHeaderAttributeKeysIfSet(headerLocation)
        .orElse(AUTH_HEADER_KEYS)
        .stream()
        .map(key -> translateRuleForUserId(key, rule.getId(), headerLocation.getParsingTarget()));
  }

  private AttributeRule translateRuleForUserId(
      String key, String ruleId, ParsingTarget parsingTarget) {
    return getRule(
        key, attributeRuleBuilder.buildActionAttributeRuleForUserId(ruleId), parsingTarget);
  }

  private AttributeRule getRule(
      String key, AttributeRule actionAttributeRule, ParsingTarget parsingTarget) {
    return attributeRuleBuilder.buildRuleForAttribute(
        key,
        attributeRuleBuilder.buildRuleForParsingTarget(
            attributeRuleBuilder.parsingTargetWithFallback(parsingTarget, "(?i)Basic:? (.*)"),
            attributeRuleBuilder.buildRuleForBase64(
                attributeRuleBuilder.buildRuleForRegexCaptureGroup("(.*):", actionAttributeRule))));
  }
}
