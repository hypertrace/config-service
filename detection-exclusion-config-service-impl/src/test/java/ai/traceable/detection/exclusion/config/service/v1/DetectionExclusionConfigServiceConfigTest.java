package ai.traceable.detection.exclusion.config.service.v1;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionConditionValidator;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesValidator;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

  @Test
  void testBothRuleListsHaveSameIdsAndNames() {
    DetectionExclusionConfigServiceConfig detectionExclusionConfigServiceConfig =
        new DetectionExclusionConfigServiceConfig(ConfigFactory.empty());
    List<DetectionExclusionRule> oldRules =
        detectionExclusionConfigServiceConfig.getDefaultDetectionExclusionRules();
    List<DetectionExclusionRule> newRules =
        detectionExclusionConfigServiceConfig.getDefaultNewDetectionExclusionRules();
    assertEquals(
        oldRules.size(),
        newRules.size(),
        String.format(
            "detectionExclusionRules (%d) and detectionExclusionNewRules (%d) should have the same number of rules",
            oldRules.size(), newRules.size()));

    Map<String, String> oldRulesMap =
        oldRules.stream()
            .collect(
                Collectors.toMap(
                    DetectionExclusionRule::getId, rule -> rule.getRuleInfo().getName()));
    Map<String, String> newRulesMap =
        newRules.stream()
            .collect(
                Collectors.toMap(
                    DetectionExclusionRule::getId, rule -> rule.getRuleInfo().getName()));

    oldRulesMap
        .keySet()
        .forEach(
            id ->
                assertTrue(
                    newRulesMap.containsKey(id),
                    String.format(
                        "Rule ID '%s' exists in detectionExclusionRules but missing in detectionExclusionNewRules",
                        id)));

    newRulesMap
        .keySet()
        .forEach(
            id ->
                assertTrue(
                    oldRulesMap.containsKey(id),
                    String.format(
                        "Rule ID '%s' exists in detectionExclusionNewRules but missing in detectionExclusionRules",
                        id)));

    oldRulesMap.forEach(
        (id, oldName) -> {
          String newName = newRulesMap.get(id);
          assertEquals(
              oldName,
              newName,
              String.format(
                  "Rule ID '%s' has different names: '%s' in detectionExclusionRules vs '%s' in detectionExclusionNewRules",
                  id, oldName, newName));
        });
  }

  private void assertDetectionExclusionRules(List<DetectionExclusionRule> detectionExclusionRules) {
    int detectionExclusionRulesCount = detectionExclusionRules.size();
    assertEquals(17, detectionExclusionRulesCount);
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
