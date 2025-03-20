package ai.traceable.external.agent.attribute.config.service.translator.userattributionv2;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.AUTH_TYPES_ATTRIBUTE_KEY;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.AUTH_TYPES_RULE_ATTRIBUTE_KEY;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleData;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor_ = {@Inject})
class UserAttributionRuleV2TranslatorImpl implements UserAttributionRuleV2Translator {
  /**
   * these attributes need to be in sync with the ones defined in {@link
   * ai.traceable.external.data.classification.config.service.userattributionv2.UserAttributionConstants}
   */
  private static final String RULE_SUFFIX = ".rule";

  private static final String CUSTOM_ATTRIBUTE_PREFIX = "traceableai.custom.attribute.";

  private final AttributeRuleBuilder attributeRuleBuilder;
  private final PredicateTranslator predicateTranslator;
  private final UserAttributionTokenRuleTranslator tokenRuleTranslator;

  @Override
  public Stream<AttributeRule> translateRuleForUserId(UserAttributionRule rule) {
    UserAttributionRuleData data = rule.getData();
    if (data.getDisabled() || !data.hasUserIdRule()) {
      return Stream.empty();
    }
    try {
      AttributeRule attributeRule =
          attributeRuleBuilder.buildActionAttributeRuleForUserId(rule.getId());
      return tokenRuleTranslator.translateTokenRule(
          predicateTranslator.buildScopePredicate(data),
          data.getUserIdRule(),
          data.getRootTokenRule(),
          attributeRule);
    } catch (Exception ex) {
      log.warn(
          String.format(
              "Unable to translate user-id rule part of user attribution rule %s", rule.getId()),
          ex);
      return Stream.empty();
    }
  }

  @Override
  public Stream<AttributeRule> translateRuleForUserRole(UserAttributionRule rule) {
    UserAttributionRuleData data = rule.getData();
    if (data.getDisabled() || !data.hasUserRoleRule()) {
      return Stream.empty();
    }
    try {
      AttributeRule attributeRule =
          attributeRuleBuilder.buildActionAttributeRuleForUserRole(rule.getId());
      return tokenRuleTranslator.translateTokenRule(
          predicateTranslator.buildScopePredicate(data),
          data.getUserRoleRule(),
          data.getRootTokenRule(),
          attributeRule);
    } catch (Exception ex) {
      log.warn(
          String.format(
              "Unable to translate user-role rule part of user attribution rule %s", rule.getId()),
          ex);
      return Stream.empty();
    }
  }

  @Override
  public Stream<AttributeRule> translateRuleForUserScope(UserAttributionRule rule) {
    UserAttributionRuleData data = rule.getData();
    if (data.getDisabled() || !data.hasUserScopeRule()) {
      return Stream.empty();
    }
    try {
      AttributeRule attributeRule =
          attributeRuleBuilder.buildActionAttributeRuleForUserScope(rule.getId());
      return tokenRuleTranslator.translateTokenRule(
          predicateTranslator.buildScopePredicate(data),
          data.getUserScopeRule(),
          data.getRootTokenRule(),
          attributeRule);
    } catch (Exception ex) {
      log.warn(
          String.format(
              "Unable to translate user-scope rule part of user attribution rule %s", rule.getId()),
          ex);
      return Stream.empty();
    }
  }

  @Override
  public Stream<AttributeRule> translateRuleForAuthType(UserAttributionRule rule) {
    UserAttributionRuleData data = rule.getData();
    if (data.getDisabled() || !data.hasAuthTypeRule()) {
      return Stream.empty();
    }
    try {
      AttributeRule attributeRule = buildActionAttributeRuleForAuthType(rule.getId());
      return tokenRuleTranslator.translateTokenRule(
          predicateTranslator.buildScopePredicate(data),
          data.getAuthTypeRule(),
          data.getRootTokenRule(),
          attributeRule);
    } catch (Exception ex) {
      log.warn(
          String.format(
              "Unable to translate auth-type rule part of user attribution rule %s", rule.getId()),
          ex);
      return Stream.empty();
    }
  }

  @Override
  public Stream<Map.Entry<String, AttributeRule>> translateRuleForCustomTokens(
      UserAttributionRule rule) {
    UserAttributionRuleData data = rule.getData();
    if (data.getDisabled() || data.getCustomTokenRulesMap().isEmpty()) {
      return Stream.empty();
    }
    return data.getCustomTokenRulesMap().entrySet().stream()
        .flatMap(
            entry -> {
              String customAttributeKey = CUSTOM_ATTRIBUTE_PREFIX + entry.getKey();
              try {
                return tokenRuleTranslator
                    .translateTokenRule(
                        entry.getValue(),
                        data.getRootTokenRule(),
                        buildActionAttributeRuleForCustomAttribute(
                            customAttributeKey, rule.getId()))
                    .map(translatedRule -> Map.entry(customAttributeKey, translatedRule));
              } catch (Exception ex) {
                log.warn(
                    String.format(
                        "Unable to translate custom-token rule %s part of user attribution rule %s",
                        entry.getKey(), rule.getId()),
                    ex);
                return Stream.empty();
              }
            });
  }

  private AttributeRule buildActionAttributeRuleForAuthType(String ruleId) {
    return AttributeRule.newBuilder()
        .addInitialActions(
            attributeRuleBuilder.buildAttributeAppendAction(AUTH_TYPES_ATTRIBUTE_KEY))
        .addInitialActions(
            attributeRuleBuilder.buildAttributeAppendAction(AUTH_TYPES_RULE_ATTRIBUTE_KEY, ruleId))
        .build();
  }

  private AttributeRule buildActionAttributeRuleForCustomAttribute(
      String attributeName, String ruleId) {
    return AttributeRule.newBuilder()
        .addInitialActions(attributeRuleBuilder.buildAttributeAdditionAction(attributeName))
        .addInitialActions(
            attributeRuleBuilder.buildAttributeAdditionAction(attributeName + RULE_SUFFIX, ruleId))
        .build();
  }
}
