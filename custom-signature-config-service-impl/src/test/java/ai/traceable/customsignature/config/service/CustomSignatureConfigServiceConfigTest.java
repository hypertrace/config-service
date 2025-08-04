package ai.traceable.customsignature.config.service;

import static ai.traceable.customsignature.config.service.v1.RuleSource.RULE_SOURCE_DEFAULT;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.customsignature.config.service.rules.ClauseGroupValidator;
import ai.traceable.customsignature.config.service.rules.CustomSignatureRulesValidator;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CustomSignatureConfigServiceConfigTest {

  private final CustomSignatureRulesValidator validator =
      new CustomSignatureRulesValidator(null, new ClauseGroupValidator());
  private final CustomSignatureConfigServiceConfig config =
      new CustomSignatureConfigServiceConfig(ConfigFactory.empty());

  @Test
  void testLoadDefaultCustomSignatureRules() {
    List<CustomSignatureRule> defaultCustomSignatureRules = config.getDefaultCustomSignatureRules();
    assertDefaultCustomSignatureRules(defaultCustomSignatureRules);
  }

  private void assertDefaultCustomSignatureRules(List<CustomSignatureRule> customSignatureRules) {

    int customSignatureRulesCount = customSignatureRules.size();
    assertEquals(2, customSignatureRulesCount);

    assertEquals(
        customSignatureRulesCount,
        customSignatureRules.stream()
            .map(CustomSignatureRule::getId)
            .filter(id -> !id.isBlank())
            .distinct()
            .count());
    assertEquals(
        customSignatureRulesCount,
        customSignatureRules.stream()
            .map(CustomSignatureRule::getName)
            .filter(name -> !name.isBlank())
            .distinct()
            .count());
    customSignatureRules.forEach(
        customSignatureRule -> assertTrue(customSignatureRule.getDisabled()));
    assertTrue(
        customSignatureRules.stream()
            .allMatch(rule -> rule.getRuleSource().equals(RULE_SOURCE_DEFAULT)));
    // rule ids conform to UUID
    assertDoesNotThrow(() -> customSignatureRules.forEach(rule -> UUID.fromString(rule.getId())));
    customSignatureRules.forEach(
        rule -> {
          UpdateCustomSignatureRuleRequest request =
              UpdateCustomSignatureRuleRequest.newBuilder()
                  .setRule(rule.toBuilder().clearRuleSource().build())
                  .build();
          assertDoesNotThrow(() -> validator.validate(request));
        });
  }
}
