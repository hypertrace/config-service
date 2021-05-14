package ai.traceable.userattribution.config.service;

import static java.util.function.Predicate.not;

import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class UserAttributionRuleDiffer {
  List<UserAttributionRule> filterUnchangedRules(
      List<UserAttributionRule> existingRules, List<UserAttributionRule> updatedRules) {
    Set<UserAttributionRule> existingRuleSet = Set.copyOf(existingRules);
    return updatedRules.stream()
        .filter(not(existingRuleSet::contains))
        .collect(Collectors.toUnmodifiableList());
  }
}
