package ai.traceable.detection.exclusion.config.service.v1.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.RuleChangeSource;
import com.google.protobuf.Value;
import java.util.Optional;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class DetectionExclusionRulesStoreTest {

  private DetectionExclusionRulesStore detectionExclusionRulesStore =
      new DetectionExclusionRulesStore(null, mock(ConfigChangeEventGenerator.class));

  @Test
  void testDataValueConversion() {
    DetectionExclusionRule rule = DetectionExclusionRule.getDefaultInstance();
    Value value = detectionExclusionRulesStore.buildValueFromData(rule);
    assertEquals(rule, detectionExclusionRulesStore.buildDataFromValue(value).get());
  }

  @Test
  void testFilterConfigData() {
    DetectionExclusionRule detectionExclusionRule =
        DetectionExclusionRule.newBuilder()
            .setId("ruleId")
            .setRuleScope(
                DetectionExclusionRuleScope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("envId1")))
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setRuleStatus(
                        DetectionExclusionRuleStatus.newBuilder()
                            .setDisabled(false)
                            .setHidden(false)
                            .setChangeSource(RuleChangeSource.RULE_CHANGE_SOURCE_CUSTOMER)
                            .build())
                    .build())
            .build();

    Optional<DetectionExclusionRule> result =
        detectionExclusionRulesStore.filterConfigData(
            detectionExclusionRule,
            getRulesFilter(
                "envId2", false, false, "ruleId", RuleChangeSource.RULE_CHANGE_SOURCE_CUSTOMER));
    assertTrue(result.isEmpty());

    result =
        detectionExclusionRulesStore.filterConfigData(
            detectionExclusionRule,
            getRulesFilter(
                "envId1", true, false, "ruleId", RuleChangeSource.RULE_CHANGE_SOURCE_CUSTOMER));
    assertTrue(result.isEmpty());

    result =
        detectionExclusionRulesStore.filterConfigData(
            detectionExclusionRule,
            getRulesFilter(
                "envId1", false, true, "ruleId", RuleChangeSource.RULE_CHANGE_SOURCE_CUSTOMER));
    assertTrue(result.isEmpty());

    result =
        detectionExclusionRulesStore.filterConfigData(
            detectionExclusionRule,
            getRulesFilter(
                "envId1", false, false, "ruleId1", RuleChangeSource.RULE_CHANGE_SOURCE_CUSTOMER));
    assertTrue(result.isEmpty());

    result =
        detectionExclusionRulesStore.filterConfigData(
            detectionExclusionRule,
            getRulesFilter(
                "envId1", false, false, "ruleId", RuleChangeSource.RULE_CHANGE_SOURCE_SYSTEM));
    assertTrue(result.isEmpty());

    result =
        detectionExclusionRulesStore.filterConfigData(
            detectionExclusionRule,
            getRulesFilter(
                "envId1", false, false, "ruleId", RuleChangeSource.RULE_CHANGE_SOURCE_CUSTOMER));
    assertEquals(detectionExclusionRule, result.get());
  }

  private GetRulesFilter getRulesFilter(
      String envId,
      boolean disabled,
      boolean hidden,
      String ruleId,
      RuleChangeSource changeSource) {
    return GetRulesFilter.newBuilder()
        .addRuleIds(ruleId)
        .setRuleScope(
            DetectionExclusionRuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds(envId)))
        .setDisabled(disabled)
        .setHidden(hidden)
        .addRuleChangeSources(changeSource)
        .build();
  }
}
