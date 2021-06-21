package ai.traceable.sensitivedata.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.sensitivedata.config.service.v1.NewRedactionRule;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DefaultRedactionRulesTest {

  @Test
  void producesCompletedStatus() {
    DefaultRedactionRules defaultRedactionRules =
        new DefaultRedactionRules(
            Map.of(
                "first",
                NewRedactionRule.getDefaultInstance(),
                "second",
                NewRedactionRule.getDefaultInstance()),
            List.of());

    assertEquals(
        DefaultRedactionRulePopulationStatus.of(Set.of("first", "second")),
        defaultRedactionRules.completedPrepopulationStatus(
            DefaultRedactionRulePopulationStatus.empty()));

    assertEquals(
        DefaultRedactionRulePopulationStatus.of(Set.of("first", "second", "other")),
        defaultRedactionRules.completedPrepopulationStatus(
            DefaultRedactionRulePopulationStatus.of(Set.of("other"))));
  }

  @Test
  void parsesConfigIntoRules() {
    String prepopulateRuleConfigString =
        "{\n"
            + "prepop-id-1: { name: prepop-name-1 },\n"
            + "prepop-id-2: { name: prepop-name-2 },\n"
            + "  }";

    String defaultRuleConfigString = "{ name: \"default-name-1\" }";

    DefaultRedactionRules defaultRedactionRules =
        new DefaultRedactionRules(
            ConfigFactory.parseString(prepopulateRuleConfigString).root(),
            List.of(ConfigFactory.parseString(defaultRuleConfigString).root()));

    assertEquals(
        List.of(NewRedactionRule.newBuilder().setName("default-name-1").build()),
        defaultRedactionRules.getDefaultRules());

    assertEquals(
        Map.of(
            "prepop-id-1",
            NewRedactionRule.newBuilder().setName("prepop-name-1").build(),
            "prepop-id-2",
            NewRedactionRule.newBuilder().setName("prepop-name-2").build()),
        defaultRedactionRules.getRulesToPrepopulate(DefaultRedactionRulePopulationStatus.empty()));

    assertEquals(
        Map.of("prepop-id-2", NewRedactionRule.newBuilder().setName("prepop-name-2").build()),
        defaultRedactionRules.getRulesToPrepopulate(
            DefaultRedactionRulePopulationStatus.of(Set.of("prepop-id-1"))));

    assertTrue(
        defaultRedactionRules.isPrepopulationComplete(
            DefaultRedactionRulePopulationStatus.of(Set.of("prepop-id-1", "prepop-id-2"))));
    assertFalse(
        defaultRedactionRules.isPrepopulationComplete(
            DefaultRedactionRulePopulationStatus.of(Set.of("prepop-id-1"))));
  }
}
