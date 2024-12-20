package ai.traceable.external.agent.attribute.config.service.translator.authdetection;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionRule;
import ai.traceable.auth.detection.config.service.v1.Predicate;
import ai.traceable.auth.detection.config.service.v1.Predicate.PredicateCase;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuthDetectionRuleTranslatorLookup {

  private final Map<PredicateCase, AuthDetectionRuleTranslator> ruleTranslatorMap;

  @Inject
  AuthDetectionRuleTranslatorLookup(Set<AuthDetectionRuleTranslator> ruleTranslators) {
    this.ruleTranslatorMap =
        ruleTranslators.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    AuthDetectionRuleTranslator::getPredicateCase, Function.identity()));
  }

  public Optional<AuthDetectionRuleTranslator> getRuleTranslator(
      AuthDetectionRule authDetectionRule) {
    if (!this.ruleTranslatorMap.containsKey(authDetectionRule.getPredicate().getPredicateCase())) {
      log.error("No translator defined for provided rule: {}", authDetectionRule);
      return Optional.empty();
    }
    return Optional.of(
        this.ruleTranslatorMap.get(authDetectionRule.getPredicate().getPredicateCase()));
  }

  public Optional<AuthDetectionRuleTranslator> getRuleTranslator(Predicate predicate) {
    if (!this.ruleTranslatorMap.containsKey(predicate.getPredicateCase())) {
      log.error("No translator defined for provided predicate: {}", predicate);
      return Optional.empty();
    }
    return Optional.of(this.ruleTranslatorMap.get(predicate.getPredicateCase()));
  }
}
