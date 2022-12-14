package ai.traceable.external.agent.attribute.config.service.translator.userattribution;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.COOKIE_HEADER_KEY;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_BODY_KEYS;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.CustomTokenRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.DataCase;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.HeaderLocation;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor_ = {@Inject})
public class CustomTokenRuleTranslator implements UserAttributionRuleTranslator {

  private final AttributeKeysExtractor attributeKeysExtractor;
  private final AttributeRuleBuilder attributeRuleBuilder;

  @Override
  public DataCase getRuleDataCase() {
    return DataCase.CUSTOM_TOKEN_DATA;
  }

  @Override
  public Stream<AttributeRule> translateRuleForAuthType(UserAttributionRule rule) {
    CustomTokenRuleData data = rule.getData().getCustomTokenData();
    switch (data.getLocationCase()) {
      case REQUEST_HEADER_LOCATION:
        return translateRuleForRequestHeaderLocation(rule);
      case REQUEST_BODY_LOCATION:
        return REQUEST_BODY_KEYS.stream()
            .map(attributeKey -> translateRuleForRequestBody(data, attributeKey, rule.getId()));
      case LOCATION_NOT_SET:
      default:
        log.error("Unrecognized location in custom token rule: {}", data.getLocationCase());
        return Stream.empty();
    }
  }

  private Stream<AttributeRule> translateRuleForRequestHeaderLocation(UserAttributionRule rule) {
    CustomTokenRuleData data = rule.getData().getCustomTokenData();
    HeaderLocation headerLocation = data.getRequestHeaderLocation();
    switch (headerLocation.getLocationCase()) {
      case HEADER_NAME:
        return attributeKeysExtractor
            .getHeaderAttributeKeys(data.getRequestHeaderLocation())
            .stream()
            .map(attributeKey -> translateRuleForRequestHeader(data, attributeKey, rule.getId()));
      case COOKIE_NAME:
        return Stream.of(
            translateRuleForRequestCookie(data, headerLocation.getCookieName(), rule.getId()));
      case LOCATION_NOT_SET:
      default:
        log.error("Unrecognized header location case: {}", headerLocation.getLocationCase());
        return Stream.empty();
    }
  }

  private AttributeRule translateRuleForRequestHeader(
      CustomTokenRuleData data, String attributeKey, String ruleId) {
    return attributeRuleBuilder.buildRuleForAttribute(
        attributeKey,
        attributeRuleBuilder.buildActionAttributeRuleForAuthType(
            data.getAuthentication().getType(), ruleId));
  }

  private AttributeRule translateRuleForRequestCookie(
      CustomTokenRuleData data, String cookieName, String ruleId) {
    return attributeRuleBuilder.buildRuleForAttribute(
        COOKIE_HEADER_KEY,
        attributeRuleBuilder.buildRuleForCookie(
            cookieName,
            attributeRuleBuilder.buildActionAttributeRuleForAuthType(
                data.getAuthentication().getType(), ruleId)));
  }

  private AttributeRule translateRuleForRequestBody(
      CustomTokenRuleData data, String attributeKey, String ruleId) {
    return attributeRuleBuilder.buildRuleForAttribute(
        attributeKey,
        attributeRuleBuilder.buildRuleForJsonPath(
            data.getRequestBodyLocation().getJsonPath(),
            attributeRuleBuilder.buildActionAttributeRuleForAuthType(
                data.getAuthentication().getType(), ruleId)));
  }
}
