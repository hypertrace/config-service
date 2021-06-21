package ai.traceable.sensitivedata.config.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.Test;

class DefaultRedactionRulePopulationStatusTest {

  @Test
  void canBuildRules() {
    DefaultRedactionRulePopulationStatus status =
        DefaultRedactionRulePopulationStatus.empty()
            .withAdditionalPopulatedRules(Set.of("k1"))
            .withAdditionalPopulatedRules(Set.of("k2"))
            .forAutomaticRedactionState(() -> false);

    assertTrue(status.hasRuleBeenPopulated("k1"));
    assertFalse(status.shouldPopulateRule("k1"));
    assertTrue(status.hasRuleBeenPopulated("k2"));
    assertFalse(status.shouldPopulateRule("k2"));

    assertFalse(status.hasRuleBeenPopulated("auto_redaction_rule_1"));
    assertFalse(status.shouldPopulateRule("auto_redaction_rule_1"));
    assertFalse(status.hasRuleBeenPopulated("other_rule_1"));
    assertTrue(status.shouldPopulateRule("other_rule_1"));

    status =
        DefaultRedactionRulePopulationStatus.empty()
            .withAdditionalPopulatedRules(Set.of("k1"))
            .withAdditionalPopulatedRules(Set.of("k2"))
            .forAutomaticRedactionState(() -> true);

    assertFalse(status.hasRuleBeenPopulated("auto_redaction_rule_1"));
    assertTrue(status.shouldPopulateRule("auto_redaction_rule_1"));
  }
}
