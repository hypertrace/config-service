package ai.traceable.external.agent.attribute.config.service.translator.userattribution;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.RESPONSE_BODY_KEYS;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.DataCase;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.ResponseBodyUserAttributionRuleData;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(onConstructor_ = {@Inject})
public class ResponseBodyRuleTranslator implements UserAttributionRuleTranslator {

  private final AttributeRuleBuilder attributeRuleBuilder;

  @Override
  public DataCase getRuleDataCase() {
    return DataCase.RESPONSE_BODY_DATA;
  }

  @Override
  public Stream<AttributeRule> translateRuleForUserId(UserAttributionRule rule) {
    return RESPONSE_BODY_KEYS.stream()
        .map(
            attributeKey ->
                translateRuleForUserId(
                    rule.getData().getResponseBodyData(), attributeKey, rule.getId()))
        .flatMap(Optional::stream);
  }

  @Override
  public Stream<AttributeRule> translateRuleForUserRole(UserAttributionRule rule) {
    return RESPONSE_BODY_KEYS.stream()
        .map(
            attributeKey ->
                translateRuleForUserRole(
                    rule.getData().getResponseBodyData(), attributeKey, rule.getId()))
        .flatMap(Optional::stream);
  }

  @Override
  public Stream<AttributeRule> translateRuleForAuthType(UserAttributionRule rule) {
    return RESPONSE_BODY_KEYS.stream()
        .map(
            attributeKey ->
                translateRuleForAuthType(
                    rule.getData().getResponseBodyData(), attributeKey, rule.getId()))
        .flatMap(Optional::stream);
  }

  private Optional<AttributeRule> translateRuleForUserId(
      ResponseBodyUserAttributionRuleData data, String attributeKey, String ruleId) {
    if (!data.hasUserIdLocation()) {
      return Optional.empty();
    }
    return Optional.of(
        attributeRuleBuilder.buildRuleForAttribute(
            attributeKey,
            attributeRuleBuilder.buildRuleForJsonPath(
                data.getUserIdLocation().getJsonPath(),
                attributeRuleBuilder.buildRuleForParsingTarget(
                    data.getUserIdLocation().getParsingTarget(),
                    attributeRuleBuilder.buildActionAttributeRuleForUserId(ruleId)))));
  }

  private Optional<AttributeRule> translateRuleForUserRole(
      ResponseBodyUserAttributionRuleData data, String attributeKey, String ruleId) {
    if (!data.hasRoleLocation()) {
      return Optional.empty();
    }
    return Optional.of(
        attributeRuleBuilder.buildRuleForAttribute(
            attributeKey,
            attributeRuleBuilder.buildRuleForJsonPath(
                data.getRoleLocation().getJsonPath(),
                attributeRuleBuilder.buildRuleForParsingTarget(
                    data.getRoleLocation().getParsingTarget(),
                    attributeRuleBuilder.buildActionAttributeRuleForUserRole(ruleId)))));
  }

  private Optional<AttributeRule> translateRuleForAuthType(
      ResponseBodyUserAttributionRuleData data, String attributeKey, String ruleId) {
    if (!data.hasUserIdLocation() || data.getAuthentication().getType().isEmpty()) {
      return Optional.empty();
    }
    // the agent should add the authType only if it is able to extract the userId
    return Optional.of(
        attributeRuleBuilder.buildRuleForAttribute(
            attributeKey,
            attributeRuleBuilder.buildRuleForJsonPath(
                data.getUserIdLocation().getJsonPath(),
                attributeRuleBuilder.buildActionAttributeRuleForAuthType(
                    data.getAuthentication().getType(), ruleId))));
  }
}
