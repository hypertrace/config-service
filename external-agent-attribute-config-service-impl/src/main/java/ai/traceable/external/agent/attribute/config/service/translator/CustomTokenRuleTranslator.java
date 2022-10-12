package ai.traceable.external.agent.attribute.config.service.translator;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_BODY_KEYS;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.CustomTokenRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.DataCase;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor_ = {@Inject})
public class CustomTokenRuleTranslator implements RuleTranslator {

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
        return attributeKeysExtractor
            .getHeaderAttributeKeys(data.getRequestHeaderLocation())
            .stream()
            .map(attributeKey -> translateRuleForRequestHeader(data, attributeKey, rule.getId()));
      case REQUEST_BODY_LOCATION:
        return REQUEST_BODY_KEYS.stream()
            .map(attributeKey -> translateRuleForRequestBody(data, attributeKey, rule.getId()));
      case LOCATION_NOT_SET:
      default:
        log.error("Unrecognized location in custom token rule: {}", data.getLocationCase());
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
