package ai.traceable.userattribution.config.service.store;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.userattribution.config.service.v1.RankUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class UserAttributionRuleRankCalculatorTest {

  private static final String RULE_ID_1 = "first-id";
  private static final String RULE_ID_2 = "second-id";
  private static final String RULE_ID_3 = "third-id";
  private static final UserAttributionRule RULE_1 =
      UserAttributionRule.newBuilder().setId(RULE_ID_1).setRank(1).build();
  private static final UserAttributionRule RULE_2 =
      UserAttributionRule.newBuilder().setId(RULE_ID_2).setRank(2).build();
  private static final UserAttributionRule RULE_3 =
      UserAttributionRule.newBuilder().setId(RULE_ID_3).setRank(3).build();

  private final UserAttributionRuleRankCalculator calculator =
      new UserAttributionRuleRankCalculator();

  @Test
  void canUpdateRulesBasedOnRerankRequest() {
    List<UserAttributionRule> ranked =
        calculator.rerankRules(
            RankUserAttributionRuleRequest.newBuilder()
                .setIdToUpdate(RULE_ID_1)
                .setPrecedingRuleId(RULE_ID_2)
                .build(),
            List.of(RULE_1, RULE_2, RULE_3));

    assertRuleOrder(ranked, RULE_ID_2, RULE_ID_1, RULE_ID_3);

    ranked =
        calculator.rerankRules(
            RankUserAttributionRuleRequest.newBuilder()
                .setIdToUpdate(RULE_ID_3)
                .setPrecedingRuleId(RULE_ID_1)
                .build(),
            List.of(RULE_1, RULE_2, RULE_3));

    assertRuleOrder(ranked, RULE_ID_1, RULE_ID_3, RULE_ID_2);

    ranked =
        calculator.rerankRules(
            RankUserAttributionRuleRequest.newBuilder().setIdToUpdate(RULE_ID_3).build(),
            List.of(RULE_1, RULE_2, RULE_3));

    assertRuleOrder(ranked, RULE_ID_3, RULE_ID_1, RULE_ID_2);
  }

  @Test
  void canAddNewRuleWithDefaultRank() {
    List<UserAttributionRule> ranked =
        calculator.rankAndMergeNewRule(
            UserAttributionRule.newBuilder().setId("new-id").build(),
            List.of(RULE_1, RULE_2, RULE_3));

    assertRuleOrder(ranked, RULE_ID_1, RULE_ID_2, RULE_ID_3, "new-id");
  }

  @Test
  void canRankRulesByOrder() {
    List<UserAttributionRule> ranked =
        calculator.rankFromOrder(
            List.of(
                RULE_1.toBuilder().setRank(2).build(),
                RULE_2.toBuilder().setRank(5).build(),
                RULE_3.toBuilder().setRank(9).build()));
    assertRuleOrder(ranked, RULE_ID_1, RULE_ID_2, RULE_ID_3);
  }

  @Test
  void canSortRulesByRank() {
    assertRuleOrder(
        Stream.of(RULE_2, RULE_3, RULE_1)
            .sorted(UserAttributionRuleRankCalculator.RULE_RANK_COMPARATOR)
            .collect(Collectors.toList()),
        RULE_ID_1,
        RULE_ID_2,
        RULE_ID_3);
  }

  private void assertRuleOrder(List<UserAttributionRule> rules, String... expectedRuleIds) {
    List<String> expectedRuleIdList = List.of(expectedRuleIds);

    for (int i = 0; i < rules.size(); i++) {
      UserAttributionRule rule = rules.get(i);
      String expectedId = expectedRuleIdList.get(i);
      int expectedRank = i + 1;
      assertEquals(expectedId, rule.getId());
      assertEquals(expectedRank, rule.getRank());
    }
  }
}
