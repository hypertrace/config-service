package ai.traceable.userattribution.config.service.store;

import ai.traceable.userattribution.config.service.v1.RankUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class UserAttributionRuleRankCalculator {
  static final Comparator<UserAttributionRule> RULE_RANK_COMPARATOR =
      Comparator.comparing(UserAttributionRule::getRank);

  private static final int HIGHEST_RANK = 1;

  /** Updates rule ranks based on the provided request and returns the list of updated rules */
  public List<UserAttributionRule> rerankRules(
      RankUserAttributionRuleRequest request, List<UserAttributionRule> currentRules) {
    UserAttributionRule ruleToUpdate =
        currentRules.stream()
            .filter(rule -> rule.getId().equals(request.getIdToUpdate()))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Could not find rule with ID to update: " + request));
    Optional<UserAttributionRule> precedingRule =
        currentRules.stream()
            .filter(rule -> rule.getId().equals(request.getPrecedingRuleId()))
            .findFirst();
    if (precedingRule.isEmpty() && request.hasPrecedingRuleId()) {
      throw new IllegalArgumentException("Preceding rule ID not found: " + request);
    }
    int oldRank = ruleToUpdate.getRank();
    // If the rule is being ranked lower (that is, higher rank value) then use the target rule's
    // rank (because the target rule will move up one rank due to the move). If it is being ranked
    // higher (lower rank value), the target rule won't move and we should increment our new rank.
    int newRank =
        precedingRule
            .map(rule -> rule.getRank() > oldRank ? rule.getRank() : rule.getRank() + 1)
            .orElse(HIGHEST_RANK);
    List<UserAttributionRule> rules = new LinkedList<>(currentRules);
    rules.remove(ruleToUpdate);
    rules.add(newRank - 1, ruleToUpdate); // Convert to index by subtracting 1
    return this.rankFromOrder(rules);
  }

  /**
   * Takes a newly created rule, assigns it a rank and merges the rule into the existing list
   * returning the complete sorted rule list.
   */
  public List<UserAttributionRule> rankAndMergeNewRule(
      UserAttributionRule newRule, List<UserAttributionRule> currentRules) {
    List<UserAttributionRule> rules = new LinkedList<>(currentRules);
    rules.add(newRule);
    return this.rankFromOrder(rules);
  }

  /** Updates the provided rules to remove any gaps in rule rankings */
  public List<UserAttributionRule> rankFromOrder(List<UserAttributionRule> rules) {
    AtomicInteger nextRank = new AtomicInteger(HIGHEST_RANK);
    return rules.stream()
        .map(rule -> this.ruleWithNewRank(rule, nextRank.getAndIncrement()))
        .collect(Collectors.toUnmodifiableList());
  }

  private UserAttributionRule ruleWithNewRank(UserAttributionRule rule, int rank) {
    return rule.toBuilder().setRank(rank).build();
  }
}
