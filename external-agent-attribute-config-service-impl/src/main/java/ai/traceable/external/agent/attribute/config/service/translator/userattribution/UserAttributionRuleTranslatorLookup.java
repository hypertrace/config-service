package ai.traceable.external.agent.attribute.config.service.translator.userattribution;

import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.DataCase;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserAttributionRuleTranslatorLookup {

  private final Map<DataCase, UserAttributionRuleTranslator> ruleTranslatorMap;

  @Inject
  UserAttributionRuleTranslatorLookup(Set<UserAttributionRuleTranslator> ruleTranslators) {
    this.ruleTranslatorMap =
        ruleTranslators.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    UserAttributionRuleTranslator::getRuleDataCase, Function.identity()));
  }

  public Optional<UserAttributionRuleTranslator> getRuleTranslator(
      UserAttributionRule userAttributionRule) {
    if (!this.ruleTranslatorMap.containsKey(userAttributionRule.getData().getDataCase())) {
      log.error("No translator defined for provided rule: {}", userAttributionRule);
      return Optional.empty();
    }
    return Optional.of(this.ruleTranslatorMap.get(userAttributionRule.getData().getDataCase()));
  }
}
