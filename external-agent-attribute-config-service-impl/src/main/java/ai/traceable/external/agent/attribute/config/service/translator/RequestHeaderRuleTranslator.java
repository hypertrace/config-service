package ai.traceable.external.agent.attribute.config.service.translator;

import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.DataCase;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RequestHeaderUserAttributionRuleData;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(onConstructor_ = {@Inject})
public class RequestHeaderRuleTranslator implements RuleTranslator {

  private final AttributeKeysExtractor attributeKeysExtractor;
  private final AttributeRuleBuilder attributeRuleBuilder;

  @Override
  public DataCase getRuleDataCase() {
    return DataCase.REQUEST_HEADER_DATA;
  }

  @Override
  public Stream<AttributeRule> translateRuleForUserId(UserAttributionRule rule) {
    return attributeKeysExtractor
        .getHeaderAttributeKeys(rule.getData().getRequestHeaderData().getUserIdLocation())
        .stream()
        .map(attributeKey -> translateRuleForUserId(attributeKey, rule.getId()));
  }

  @Override
  public Stream<AttributeRule> translateRuleForUserRole(UserAttributionRule rule) {
    // since role is optional, role location may not be present
    return attributeKeysExtractor
        .getHeaderAttributeKeysIfSet(rule.getData().getRequestHeaderData().getRoleLocation())
        .orElse(Collections.emptyList())
        .stream()
        .map(attributeKey -> translateRuleForUserRole(attributeKey, rule.getId()));
  }

  @Override
  public Stream<AttributeRule> translateRuleForAuthType(UserAttributionRule rule) {
    // the agent should add the authType only if it is able to extract the userId
    return attributeKeysExtractor
        .getHeaderAttributeKeys(rule.getData().getRequestHeaderData().getUserIdLocation())
        .stream()
        .map(
            attributeKey ->
                translateRuleForAuthType(
                    rule.getData().getRequestHeaderData(), attributeKey, rule.getId()))
        .flatMap(Optional::stream);
  }

  private AttributeRule translateRuleForUserId(String attributeKey, String ruleId) {
    return attributeRuleBuilder.buildRuleForAttribute(
        attributeKey, attributeRuleBuilder.buildActionAttributeRuleForUserId(ruleId));
  }

  private AttributeRule translateRuleForUserRole(String attributeKey, String ruleId) {
    return attributeRuleBuilder.buildRuleForAttribute(
        attributeKey, attributeRuleBuilder.buildActionAttributeRuleForUserRole(ruleId));
  }

  private Optional<AttributeRule> translateRuleForAuthType(
      RequestHeaderUserAttributionRuleData data, String attributeKey, String ruleId) {
    if (data.getAuthentication().getType().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        attributeRuleBuilder.buildRuleForAttribute(
            attributeKey,
            attributeRuleBuilder.buildActionAttributeRuleForAuthType(
                data.getAuthentication().getType(), ruleId)));
  }
}
