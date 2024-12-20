package ai.traceable.external.agent.attribute.config.service.translator.userattribution;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.DataCase;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.HeaderLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.ParsingTarget;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RequestHeaderUserAttributionRuleData;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(onConstructor_ = {@Inject})
public class RequestHeaderRuleTranslator implements UserAttributionRuleTranslator {

  private final AttributeKeysExtractor attributeKeysExtractor;
  private final AttributeRuleBuilder attributeRuleBuilder;

  @Override
  public DataCase getRuleDataCase() {
    return DataCase.REQUEST_HEADER_DATA;
  }

  @Override
  public Stream<AttributeRule> translateRuleForUserId(UserAttributionRule rule) {
    HeaderLocation userIdLocation = rule.getData().getRequestHeaderData().getUserIdLocation();
    return attributeKeysExtractor.getHeaderAttributeKeys(userIdLocation).stream()
        .map(
            attributeKey ->
                translateRuleForUserId(
                    attributeKey, rule.getId(), userIdLocation.getParsingTarget()));
  }

  @Override
  public Stream<AttributeRule> translateRuleForUserRole(UserAttributionRule rule) {
    // since role is optional, role location may not be present
    HeaderLocation roleLocation = rule.getData().getRequestHeaderData().getRoleLocation();
    return attributeKeysExtractor
        .getHeaderAttributeKeysIfSet(roleLocation)
        .orElse(Collections.emptyList())
        .stream()
        .map(
            attributeKey ->
                translateRuleForUserRole(
                    attributeKey, rule.getId(), roleLocation.getParsingTarget()));
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

  private AttributeRule translateRuleForUserId(
      String attributeKey, String ruleId, ParsingTarget parsingTarget) {
    return attributeRuleBuilder.buildRuleForAttribute(
        attributeKey,
        attributeRuleBuilder.buildRuleForParsingTarget(
            parsingTarget, attributeRuleBuilder.buildActionAttributeRuleForUserId(ruleId)));
  }

  private AttributeRule translateRuleForUserRole(
      String attributeKey, String ruleId, ParsingTarget parsingTarget) {
    return attributeRuleBuilder.buildRuleForAttribute(
        attributeKey,
        attributeRuleBuilder.buildRuleForParsingTarget(
            parsingTarget, attributeRuleBuilder.buildActionAttributeRuleForUserRole(ruleId)));
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
