package ai.traceable.external.agent.attribute.config.service.translator;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionRule;
import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.JwtExtractionRuleTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.userattribution.UserAttributionRuleTranslatorLookup;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor_ = {@Inject})
public class ExternalAgentAttributeRuleTranslator {

  private final AttributeRuleBuilder attributeRuleBuilder;
  private final UrlScopeTranslator urlScopeTranslator;
  private final AuthTypeAttributeRuleBuilder authTypeRuleBuilder;

  private final UserAttributionRuleTranslatorLookup ruleTranslatorLookup;
  private final JwtExtractionRuleTranslator jwtExtractionRuleTranslator;

  public List<AttributeRule> translateRules(
      List<UserAttributionRule> userAttributionRules,
      List<AuthDetectionRule> authDetectionRules,
      List<JwtExtractionRule> jwtExtractionRules) {
    List<AttributeRule> agentAttributeRules = new ArrayList<>();
    getAttributeRuleForUserId(userAttributionRules).ifPresent(agentAttributeRules::add);
    getAttributeRuleForUserRole(userAttributionRules).ifPresent(agentAttributeRules::add);
    // Auth type rules come from both UA and auth detection rules, so delegated to a separate class
    authTypeRuleBuilder
        .buildRule(userAttributionRules, authDetectionRules)
        .ifPresent(agentAttributeRules::add);
    jwtExtractionRuleTranslator
        .translateJwtExtractionRules(jwtExtractionRules)
        .forEach(agentAttributeRules::add);
    return Collections.unmodifiableList(agentAttributeRules);
  }

  public List<AttributeRule> condenseToSingleRule(List<AttributeRule> rules) {
    return rules.isEmpty()
        ? Collections.emptyList()
        : List.of(attributeRuleBuilder.buildRuleForEachMatchingProjector(rules));
  }

  private Optional<AttributeRule> getAttributeRuleForUserId(List<UserAttributionRule> rules) {
    return collectAnyTranslatedRules(
            rules,
            rule ->
                this.ruleTranslatorLookup.getRuleTranslator(rule).stream()
                    .flatMap(translator -> translator.translateRuleForUserId(rule)))
        .map(attributeRuleBuilder::buildRuleForFirstMatchingProjector);
  }

  private Optional<AttributeRule> getAttributeRuleForUserRole(List<UserAttributionRule> rules) {
    return collectAnyTranslatedRules(
            rules,
            rule ->
                this.ruleTranslatorLookup.getRuleTranslator(rule).stream()
                    .flatMap(translator -> translator.translateRuleForUserRole(rule)))
        .map(attributeRuleBuilder::buildRuleForFirstMatchingProjector);
  }

  private Optional<List<AttributeRule>> collectAnyTranslatedRules(
      List<UserAttributionRule> rules,
      Function<UserAttributionRule, Stream<AttributeRule>> ruleTranslator) {
    List<AttributeRule> attributeRules =
        rules.stream()
            .flatMap(rule -> translateRule(rule, ruleTranslator))
            .collect(Collectors.toUnmodifiableList());
    return attributeRules.isEmpty() ? Optional.empty() : Optional.of(attributeRules);
  }

  private Stream<AttributeRule> translateRule(
      UserAttributionRule rule,
      Function<UserAttributionRule, Stream<AttributeRule>> ruleTranslator) {
    return ruleTranslator
        .apply(rule)
        .map(
            translatedRule -> urlScopeTranslator.addUrlScopeIfSet(rule.getScope(), translatedRule));
  }
}
