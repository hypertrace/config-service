package ai.traceable.external.agent.attribute.config.service.translator;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionRule;
import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.JwtExtractionRuleTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.servicenaming.ServiceNamingRuleTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.SessionIdentificationRuleTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.userattribution.UserAttributionRuleTranslatorLookup;
import ai.traceable.external.agent.attribute.config.service.translator.userattributionv2.UserAttributionRuleV2Translator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
  private final ServiceNamingRuleTranslator serviceNamingRuleTranslator;
  private final SessionIdentificationRuleTranslator sessionIdentificationRuleTranslator;
  private final UserAttributionRuleV2Translator userAttributionRuleV2Translator;

  public List<AttributeRule> translateRules(
      List<UserAttributionRule> userAttributionRules,
      List<ai.traceable.userattribution.config.service.v2.UserAttributionRule>
          userAttributionRulesV2,
      List<AuthDetectionRule> authDetectionRules,
      List<JwtExtractionRule> jwtExtractionRules,
      List<ServiceNamingRule> serviceNamingRules,
      List<SessionIdentificationRule> sessionIdentificationRules) {
    List<AttributeRule> agentAttributeRules = new ArrayList<>();
    getAttributeRuleForUserId(userAttributionRules, userAttributionRulesV2)
        .ifPresent(agentAttributeRules::add);
    getAttributeRuleForUserRole(userAttributionRules, userAttributionRulesV2)
        .ifPresent(agentAttributeRules::add);
    // Auth type rules come from both UA and auth detection rules, so delegated to a separate class
    authTypeRuleBuilder
        .buildRule(userAttributionRules, userAttributionRulesV2, authDetectionRules)
        .ifPresent(agentAttributeRules::add);
    // collect custom token rules
    agentAttributeRules.addAll(getAttributeRuleForCustomTokens(userAttributionRulesV2));

    jwtExtractionRuleTranslator
        .translateJwtExtractionRules(jwtExtractionRules)
        .forEach(agentAttributeRules::add);

    serviceNamingRuleTranslator.buildRule(serviceNamingRules).ifPresent(agentAttributeRules::add);

    sessionIdentificationRuleTranslator
        .translateSessionIdentificationRules(sessionIdentificationRules)
        .forEach(agentAttributeRules::add);

    return List.copyOf(agentAttributeRules);
  }

  private Optional<AttributeRule> getAttributeRuleForUserId(
      List<UserAttributionRule> rules,
      List<ai.traceable.userattribution.config.service.v2.UserAttributionRule> rulesV2) {
    return collectAnyTranslatedRules(
            rules,
            rule ->
                this.ruleTranslatorLookup.getRuleTranslator(rule).stream()
                    .flatMap(translator -> translator.translateRuleForUserId(rule)),
            rulesV2,
            userAttributionRuleV2Translator::translateRuleForUserId)
        .map(attributeRuleBuilder::buildRuleForFirstMatchingProjector);
  }

  private Optional<AttributeRule> getAttributeRuleForUserRole(
      List<UserAttributionRule> rules,
      List<ai.traceable.userattribution.config.service.v2.UserAttributionRule> rulesV2) {
    return collectAnyTranslatedRules(
            rules,
            rule ->
                this.ruleTranslatorLookup.getRuleTranslator(rule).stream()
                    .flatMap(translator -> translator.translateRuleForUserRole(rule)),
            rulesV2,
            userAttributionRuleV2Translator::translateRuleForUserRole)
        .map(attributeRuleBuilder::buildRuleForFirstMatchingProjector);
  }

  private Optional<List<AttributeRule>> collectAnyTranslatedRules(
      List<UserAttributionRule> rules,
      Function<UserAttributionRule, Stream<AttributeRule>> ruleTranslator,
      List<ai.traceable.userattribution.config.service.v2.UserAttributionRule> rulesV2,
      Function<
              ai.traceable.userattribution.config.service.v2.UserAttributionRule,
              Stream<AttributeRule>>
          ruleV2Translator) {
    Stream<AttributeRule> attributeRulesV1 =
        rules.stream().flatMap(rulev1 -> translateRule(rulev1, ruleTranslator));
    Stream<AttributeRule> attributeRulesV2 = rulesV2.stream().flatMap(ruleV2Translator::apply);
    List<AttributeRule> attributeRules =
        Stream.concat(attributeRulesV1, attributeRulesV2).collect(Collectors.toUnmodifiableList());
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

  private List<AttributeRule> getAttributeRuleForCustomTokens(
      List<ai.traceable.userattribution.config.service.v2.UserAttributionRule> rulesV2) {
    Map<String, List<Map.Entry<String, AttributeRule>>> groupedAttributeRules =
        rulesV2.stream()
            .flatMap(userAttributionRuleV2Translator::translateRuleForCustomTokens)
            .collect(Collectors.groupingBy(Map.Entry::getKey));
    return groupedAttributeRules.values().stream()
        .map(
            values -> {
              List<AttributeRule> attributeRules =
                  values.stream().map(Map.Entry::getValue).collect(Collectors.toUnmodifiableList());
              return attributeRuleBuilder.buildRuleForFirstMatchingProjector(attributeRules);
            })
        .collect(Collectors.toUnmodifiableList());
  }
}
