package ai.traceable.external.agent.attribute.config.service.translator;

import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRules;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.DataCase;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExternalAgentAttributeRuleTranslator {

  private final Map<DataCase, RuleTranslator> ruleTranslatorMap;
  private final AttributeRuleBuilder attributeRuleBuilder;

  @Inject
  public ExternalAgentAttributeRuleTranslator(
      Set<RuleTranslator> ruleTranslators, AttributeRuleBuilder attributeRuleBuilder) {
    this.ruleTranslatorMap =
        ruleTranslators.stream()
            .collect(
                Collectors.toUnmodifiableMap(RuleTranslator::getRuleDataCase, Function.identity()));
    this.attributeRuleBuilder = attributeRuleBuilder;
  }

  public AgentAttributeRules translateRules(List<UserAttributionRule> rules) {
    List<AttributeRule> attributeRulesForFields = new ArrayList<>();
    getAttributeRuleForUserId(rules).ifPresent(attributeRulesForFields::add);
    getAttributeRuleForUserRole(rules).ifPresent(attributeRulesForFields::add);
    getAttributeRuleForAuthType(rules).ifPresent(attributeRulesForFields::add);
    return attributeRulesForFields.isEmpty()
        ? AgentAttributeRules.getDefaultInstance()
        : AgentAttributeRules.newBuilder()
            .addAgentAttributeRule(
                AgentAttributeRule.newBuilder()
                    .setRootRule(
                        attributeRuleBuilder.buildRuleForEachMatchingProjector(
                            attributeRulesForFields)))
            .build();
  }

  private Optional<AttributeRule> getAttributeRuleForUserId(List<UserAttributionRule> rules) {
    return collectAnyTranslatedRules(
            rules,
            rule ->
                ruleTranslatorMap.get(rule.getData().getDataCase()).translateRuleForUserId(rule))
        .map(attributeRuleBuilder::buildRuleForFirstMatchingProjector);
  }

  private Optional<AttributeRule> getAttributeRuleForUserRole(List<UserAttributionRule> rules) {
    return collectAnyTranslatedRules(
            rules,
            rule ->
                ruleTranslatorMap.get(rule.getData().getDataCase()).translateRuleForUserRole(rule))
        .map(attributeRuleBuilder::buildRuleForFirstMatchingProjector);
  }

  private Optional<AttributeRule> getAttributeRuleForAuthType(List<UserAttributionRule> rules) {
    return collectAnyTranslatedRules(
            rules,
            rule ->
                ruleTranslatorMap.get(rule.getData().getDataCase()).translateRuleForAuthType(rule))
        .map(attributeRuleBuilder::buildRuleForEachMatchingProjector);
  }

  private Optional<List<AttributeRule>> collectAnyTranslatedRules(
      List<UserAttributionRule> rules,
      Function<UserAttributionRule, Stream<AttributeRule>> ruleTranslator) {
    List<AttributeRule> attributeRules =
        rules.stream().flatMap(ruleTranslator).collect(Collectors.toUnmodifiableList());
    return attributeRules.isEmpty() ? Optional.empty() : Optional.of(attributeRules);
  }
}
