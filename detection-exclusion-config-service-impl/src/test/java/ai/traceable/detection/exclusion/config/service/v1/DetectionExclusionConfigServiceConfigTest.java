package ai.traceable.detection.exclusion.config.service.v1;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionConditionValidator;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesValidator;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import org.junit.jupiter.api.Test;

class DetectionExclusionConfigServiceConfigTest {

  private final DetectionExclusionRulesValidator rulesValidator =
      new DetectionExclusionRulesValidator(new DetectionExclusionConditionValidator());

  @Test
  void testConfig() {
    DetectionExclusionConfigServiceConfig detectionExclusionConfigServiceConfig =
        new DetectionExclusionConfigServiceConfig(ConfigFactory.empty());
    List<DetectionExclusionRule> detectionExclusionRules =
        detectionExclusionConfigServiceConfig.getDefaultDetectionExclusionRules();
    assertDetectionExclusionRules(detectionExclusionRules);
  }

  private void assertDetectionExclusionRules(List<DetectionExclusionRule> detectionExclusionRules) {
    int detectionExclusionRulesCount = detectionExclusionRules.size();
    assertEquals(11, detectionExclusionRulesCount);
    assertTrue(
        detectionExclusionRules.stream()
            .allMatch(
                detectionExclusionRule ->
                    detectionExclusionRule
                        .getRuleInfo()
                        .getRuleStatus()
                        .getRuleCreationSource()
                        .equals(RuleSource.RULE_SOURCE_DEFAULT)));
    assertEquals(
        detectionExclusionRulesCount,
        detectionExclusionRules.stream()
            .map(DetectionExclusionRule::getId)
            .filter(id -> !id.isBlank())
            .distinct()
            .count());
    assertEquals(
        detectionExclusionRulesCount,
        detectionExclusionRules.stream()
            .map(rule -> rule.getRuleInfo().getName())
            .filter(name -> !name.isBlank())
            .distinct()
            .count());
    assertDoesNotThrow(
        () ->
            detectionExclusionRules.forEach(
                rule -> rulesValidator.validateRuleInfo(rule.getRuleInfo())));
  }
}
