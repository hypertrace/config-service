package ai.traceable.external.agent.attribute.config.service.translator;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionRule;
import ai.traceable.external.agent.attribute.config.service.translator.authdetection.AuthDetectionRuleTranslatorLookup;
import ai.traceable.external.agent.attribute.config.service.translator.userattribution.UserAttributionRuleTranslatorLookup;
import ai.traceable.external.agent.attribute.config.service.translator.userattributionv2.UserAttributionRuleV2Translator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor_ = {@Inject})
class AuthTypeAttributeRuleBuilder {

  private final UrlScopeTranslator urlScopeTranslator;
  private final AttributeRuleBuilder attributeRuleBuilder;
  private final UserAttributionRuleTranslatorLookup userAttributionRuleTranslatorLookup;
  private final UserAttributionRuleV2Translator userAttributionRuleV2Translator;
  private final AuthDetectionRuleTranslatorLookup authDetectionRuleTranslatorLookup;

  Optional<AttributeRule> buildRule(
      List<UserAttributionRule> userAttributionRules,
      List<ai.traceable.userattribution.config.service.v2.UserAttributionRule>
          userAttributionRulesV2,
      List<AuthDetectionRule> authDetectionRules) {
    return Stream.of(
            translateUserAttributionRules(userAttributionRules),
            translateUserAttributionRulesV2(userAttributionRulesV2),
            translateAuthDetectionRules(authDetectionRules))
        .flatMap(s -> s)
        .collect(collectingAndThen(toUnmodifiableList(), Optional::of))
        .filter(not(Collection::isEmpty))
        .map(attributeRuleBuilder::buildRuleForEachMatchingProjector);
  }

  private Stream<AttributeRule> translateUserAttributionRules(
      List<UserAttributionRule> userAttributionRules) {
    return userAttributionRules.stream().flatMap(this::translateUserAttributionRule);
  }

  private Stream<AttributeRule> translateUserAttributionRulesV2(
      List<ai.traceable.userattribution.config.service.v2.UserAttributionRule>
          userAttributionRulesV2) {
    return userAttributionRulesV2.stream()
        .flatMap(userAttributionRuleV2Translator::translateRuleForAuthType);
  }

  private Stream<AttributeRule> translateUserAttributionRule(
      UserAttributionRule userAttributionRule) {
    return this.userAttributionRuleTranslatorLookup.getRuleTranslator(userAttributionRule).stream()
        .flatMap(translator -> translator.translateRuleForAuthType(userAttributionRule))
        .map(
            translatedRule ->
                urlScopeTranslator.addUrlScopeIfSet(
                    userAttributionRule.getScope(), translatedRule));
  }

  private Stream<AttributeRule> translateAuthDetectionRules(
      List<AuthDetectionRule> authDetectionRules) {
    return authDetectionRules.stream()
        .filter(AuthDetectionRule::hasAuthType) // TODO add support for fallback rules later
        .flatMap(this::translateAuthDetectionRule);
  }

  private Stream<AttributeRule> translateAuthDetectionRule(AuthDetectionRule authDetectionRule) {
    return this.authDetectionRuleTranslatorLookup.getRuleTranslator(authDetectionRule).stream()
        .flatMap(translator -> translator.translateRuleForAuthType(authDetectionRule));
  }
}
