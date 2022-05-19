package ai.traceable.sensitivedata.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DefaultRedactionRulesTest {

  @Test
  void parsesConfigIntoRules() {
    String defaultRuleConfigString =
        "{\n"
            + "rule-id-1: { name: rule-name-1 },\n"
            + "rule-id-2: { name: rule-name-2 },\n"
            + "  }";

    DefaultRedactionRules defaultRedactionRules =
        new DefaultRedactionRules(ConfigFactory.parseString(defaultRuleConfigString).root());

    assertEquals(
        Optional.of(RedactionRule.newBuilder().setId("rule-id-1").setName("rule-name-1").build()),
        defaultRedactionRules.getRule("rule-id-1"));
    assertEquals(Optional.empty(), defaultRedactionRules.getRule("rule-id-fake"));

    assertEquals(
        List.of(
            RedactionRule.newBuilder().setId("rule-id-1").setName("rule-name-1").build(),
            RedactionRule.newBuilder().setId("rule-id-2").setName("rule-name-2").build()),
        defaultRedactionRules.getUnpersistedRules(DefaultRedactionRulePersistenceStatus.empty()));

    assertEquals(
        List.of(RedactionRule.newBuilder().setId("rule-id-2").setName("rule-name-2").build()),
        defaultRedactionRules.getUnpersistedRules(
            DefaultRedactionRulePersistenceStatus.of(Set.of("rule-id-1"))));

    assertTrue(defaultRedactionRules.isDefaultRuleId("rule-id-1"));
    assertFalse(defaultRedactionRules.isDefaultRuleId("rule-id-fake"));
  }
}
