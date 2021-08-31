package ai.traceable.config.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.config.utils.RankCalculator.RankConfig;
import java.util.List;
import lombok.Builder;
import lombok.Value;
import org.junit.jupiter.api.Test;

class RankCalculatorTest {

  private static final String RULE_ID_1 = "first-id";
  private static final String RULE_ID_2 = "second-id";
  private static final String RULE_ID_3 = "third-id";
  private static final TestRule RULE_1 = new TestRule(RULE_ID_1, 1);
  private static final TestRule RULE_2 = new TestRule(RULE_ID_2, 2);
  private static final TestRule RULE_3 = new TestRule(RULE_ID_3, 3);

  private final RankCalculator<TestRule, String> calculator =
      new RankCalculator<>(
          new RankConfig<>(
              TestRule::getRank,
              TestRule::getId,
              (rule, rank) -> rule.toBuilder().rank(rank).build()));

  @Test
  void canUpdateRulesBasedOnRerankRequest() {
    assertRuleOrder(
        calculator.rerankAfterOtherObject(RULE_ID_1, RULE_ID_2, List.of(RULE_1, RULE_2, RULE_3)),
        RULE_ID_2,
        RULE_ID_1,
        RULE_ID_3);

    assertRuleOrder(
        calculator.rerankAfterOtherObject(RULE_ID_3, RULE_ID_1, List.of(RULE_1, RULE_2, RULE_3)),
        RULE_ID_1,
        RULE_ID_3,
        RULE_ID_2);

    assertRuleOrder(
        calculator.rerankAsHighestRank(RULE_ID_3, List.of(RULE_1, RULE_2, RULE_3)),
        RULE_ID_3,
        RULE_ID_1,
        RULE_ID_2);
  }

  @Test
  void canAddNewRuleWithDefaultRank() {
    List<TestRule> ranked =
        calculator.rankAndMergeNewObject(
            TestRule.builder().id("new-id").build(), List.of(RULE_1, RULE_2, RULE_3));

    assertRuleOrder(ranked, RULE_ID_1, RULE_ID_2, RULE_ID_3, "new-id");
  }

  @Test
  void canRankRulesByOrder() {
    List<TestRule> ranked =
        calculator.rankFromOrder(
            List.of(
                RULE_1.toBuilder().rank(2).build(),
                RULE_2.toBuilder().rank(5).build(),
                RULE_3.toBuilder().rank(9).build()));
    assertRuleOrder(ranked, RULE_ID_1, RULE_ID_2, RULE_ID_3);
  }

  @Test
  void canSortRulesByRank() {
    assertRuleOrder(
        calculator.orderFromRanks(List.of(RULE_2, RULE_3, RULE_1)),
        RULE_ID_1,
        RULE_ID_2,
        RULE_ID_3);
  }

  private void assertRuleOrder(List<TestRule> rules, String... expectedRuleIds) {
    List<String> expectedRuleIdList = List.of(expectedRuleIds);

    for (int i = 0; i < rules.size(); i++) {
      TestRule rule = rules.get(i);
      String expectedId = expectedRuleIdList.get(i);
      int expectedRank = i + 1;
      assertEquals(expectedId, rule.getId());
      assertEquals(expectedRank, rule.getRank());
    }
  }

  @Builder(toBuilder = true)
  @Value
  private static class TestRule {
    String id;
    int rank;
  }
}
