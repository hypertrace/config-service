package ai.traceable.anomaly.config.service.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import org.junit.jupiter.api.Test;

class AnomalySubRuleConfigUtilsTest {

  @Test
  void testMergeWithBackwardCompatibility_oldFieldSet() {
    AnomalyConfigStatusChange oldStatus =
        AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();

    AnomalySubRuleConfig oldConfig =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("test-sub-rule")
            .setConfigStatus(oldStatus)
            .build();
    AnomalySubRuleConfig result = AnomalySubRuleConfigUtils.populateNewFields(oldConfig);
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE, result.getAnomalyRuleAction());
    assertTrue(result.hasConfigStatus());
    assertTrue(result.getConfigStatus().getDisabled());
  }

  @Test
  void testMergeWithBackwardCompatibility_oldFieldEnabledWithBlocking() {
    AnomalyConfigStatusChange oldStatus =
        AnomalyConfigStatusChange.newBuilder().setDisabled(false).build();

    AnomalySubRuleConfig oldConfig =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("test-sub-rule")
            .setConfigStatus(oldStatus)
            .setBlockingEnabled(true)
            .build();
    AnomalySubRuleConfig result = AnomalySubRuleConfigUtils.populateNewFields(oldConfig);
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK, result.getAnomalyRuleAction());
    assertFalse(result.getConfigStatus().getDisabled());
    assertTrue(result.getBlockingEnabled());
  }

  @Test
  void testMergeWithBackwardCompatibility_oldFieldEnabledWithoutBlocking() {
    AnomalyConfigStatusChange oldStatus =
        AnomalyConfigStatusChange.newBuilder().setDisabled(false).build();

    AnomalySubRuleConfig oldConfig =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("test-sub-rule")
            .setConfigStatus(oldStatus)
            .setBlockingEnabled(false)
            .build();
    AnomalySubRuleConfig result = AnomalySubRuleConfigUtils.populateNewFields(oldConfig);
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR, result.getAnomalyRuleAction());
    assertFalse(result.getConfigStatus().getDisabled());
    assertFalse(result.getBlockingEnabled());
  }

  @Test
  void testMergeWithBackwardCompatibility_onlyBlockingEnabled() {
    AnomalySubRuleConfig oldConfig =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("test-sub-rule")
            .setBlockingEnabled(true)
            .build();
    AnomalySubRuleConfig result = AnomalySubRuleConfigUtils.populateNewFields(oldConfig);
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK, result.getAnomalyRuleAction());
  }

  @Test
  void testMergeWithBackwardCompatibility_bothFieldsDontNeedMigration() {
    AnomalySubRuleConfig config =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("test-sub-rule")
            .setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR)
            .build();
    AnomalySubRuleConfig result = AnomalySubRuleConfigUtils.populateNewFields(config);
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR, result.getAnomalyRuleAction());
  }

  @Test
  void testMergeWithBackwardCompatibility_withInternalFlag() {
    AnomalyConfigStatusChange oldStatus =
        AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build();

    AnomalySubRuleConfig oldConfig =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("test-sub-rule")
            .setConfigStatus(oldStatus)
            .build();
    AnomalySubRuleConfig result = AnomalySubRuleConfigUtils.populateNewFields(oldConfig);

    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE, result.getAnomalyRuleAction());
    assertTrue(result.hasInternal());
    assertTrue(result.getInternal());
  }

  @Test
  void testMergeWithBackwardCompatibility_mergeOfTwoConfigs() {
    AnomalySubRuleConfig fallbackConfig =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("test-sub-rule")
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(true).build())
            .build();

    AnomalySubRuleConfig preferredConfig =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("test-sub-rule")
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(false).build())
            .setBlockingEnabled(true)
            .build();
    AnomalySubRuleConfig result =
        AnomalySubRuleConfigUtils.mergeAndPopulateNewFields(fallbackConfig, preferredConfig);
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK, result.getAnomalyRuleAction());
    assertTrue(result.hasBlockingEnabled());
    assertTrue(result.getBlockingEnabled());
  }
}
