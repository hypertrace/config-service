package ai.traceable.userattribution.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import java.util.List;
import org.junit.jupiter.api.Test;

class UserAttributionRuleDifferTest {
  private static final UserAttributionRule RULE_1 =
      UserAttributionRule.newBuilder().setId("first").setName("first").build();
  private static final UserAttributionRule RULE_2 =
      UserAttributionRule.newBuilder().setId("second").setName("second").build();
  private static final List<UserAttributionRule> RULE_1_AND_2 = List.of(RULE_1, RULE_2);

  private final UserAttributionRuleDiffer differ = new UserAttributionRuleDiffer();

  @Test
  void detectsUpdatedRules() {
    UserAttributionRule updatedRule2 = RULE_2.toBuilder().setName("second-updated").build();
    assertEquals(
        List.of(updatedRule2),
        differ.filterUnchangedRules(RULE_1_AND_2, List.of(RULE_1, updatedRule2)));
  }

  @Test
  void detectsNewRules() {
    assertEquals(List.of(RULE_2), differ.filterUnchangedRules(List.of(RULE_1), RULE_1_AND_2));
  }
}
