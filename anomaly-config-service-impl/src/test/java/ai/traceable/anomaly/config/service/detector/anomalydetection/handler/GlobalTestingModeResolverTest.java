package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.global.version.RuleVersionManager;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.RuleTestingMode;
import ai.traceable.anomaly.config.service.v1.RuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.RuleVersionData;
import ai.traceable.anomaly.config.service.v1.RuleVersionType;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalModsecConfig;
import ai.traceable.anomaly.config.service.v1.global.RulesChangeLog;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.global.ThreatRuleChange;
import ai.traceable.anomaly.config.service.v1.global.ThreatRuleUpdateDetails;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GlobalTestingModeResolverTest {

  private RuleVersionManager ruleVersionManager;
  private GlobalTestingModeResolver resolver;

  @BeforeEach
  void setUp() {
    ruleVersionManager = mock(RuleVersionManager.class);
    resolver = new GlobalTestingModeResolver(ruleVersionManager);
  }

  @Test
  void testResolveGlobalTestingMode_NoGlobalConfig() {
    Optional<ScopedAnomalyDetectionConfig> result =
        resolver.resolveGlobalTestingMode(
            ScopedAnomalyDetectionConfig.getDefaultInstance(), Optional.empty());
    assertFalse(result.isPresent());
  }

  @Test
  void testResolveGlobalTestingMode_TestingModeDisabled() {
    ScopedAnomalyConfigStatus status =
        createGlobalConfigStatus(RuleTestingMode.RULE_TESTING_MODE_DISABLED);

    Optional<ScopedAnomalyDetectionConfig> result =
        resolver.resolveGlobalTestingMode(
            ScopedAnomalyDetectionConfig.getDefaultInstance(), Optional.of(status));

    assertFalse(result.isPresent());
  }

  @Test
  void testResolveGlobalTestingMode_NewRules() {
    ScopedAnomalyDetectionConfig inputConfig =
        createScopedConfig("new-rule-1", AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR);

    ScopedAnomalyConfigStatus status =
        createGlobalConfigStatus(RuleTestingMode.RULE_TESTING_MODE_ENABLED_FOR_NEW_RULES);
    RulesChangeLog changelog =
        RulesChangeLog.newBuilder()
            .addRuleChanges(
                ThreatRuleChange.newBuilder()
                    .setRuleIdsAdded(StringList.newBuilder().addValues("new-rule-1").build())
                    .build())
            .build();

    when(ruleVersionManager.getRulesChangeLog(
            any(RuleType.class), any(RuleVersion.class), any(RuleVersion.class)))
        .thenReturn(changelog);

    Optional<ScopedAnomalyDetectionConfig> result =
        resolver.resolveGlobalTestingMode(inputConfig, Optional.of(status));

    assertTrue(result.isPresent());
    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_TESTING,
        result
            .get()
            .getAnomalyDetectionConfigs(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getSubRuleConfigs(0)
            .getAnomalyRuleAction());
  }

  @Test
  void testResolveGlobalTestingMode_UpdatedRules() {
    ScopedAnomalyDetectionConfig inputConfig =
        createScopedConfig("updated-rule-1", AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR);
    ScopedAnomalyConfigStatus status =
        createGlobalConfigStatus(RuleTestingMode.RULE_TESTING_MODE_ENABLED_FOR_UPDATED_RULES);
    RulesChangeLog changelog =
        RulesChangeLog.newBuilder()
            .addRuleChanges(
                ThreatRuleChange.newBuilder()
                    .setRuleUpdated(
                        ThreatRuleUpdateDetails.newBuilder()
                            .setRuleId("updated-rule-1")
                            .addUpdates(
                                ThreatRuleUpdateDetails.ThreatRuleUpdate.newBuilder()
                                    .setSignatureUpdated(true)
                                    .build())
                            .build())
                    .build())
            .build();

    when(ruleVersionManager.getRulesChangeLog(
            any(RuleType.class), any(RuleVersion.class), any(RuleVersion.class)))
        .thenReturn(changelog);

    Optional<ScopedAnomalyDetectionConfig> result =
        resolver.resolveGlobalTestingMode(inputConfig, Optional.of(status));

    assertTrue(result.isPresent());
    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_TESTING,
        result
            .get()
            .getAnomalyDetectionConfigs(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getSubRuleConfigs(0)
            .getAnomalyRuleAction());
  }

  @Test
  void testResolveGlobalTestingMode_NewAndUpdatedRules() {
    ScopedAnomalyDetectionConfig inputConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setModsecurityAnomalyDetectionConfig(
                        ModsecurityAnomalyDetectionConfig.newBuilder()
                            .setModsecAnomalyRule(
                                ModsecurityAnomalyRuleConfig.newBuilder()
                                    .addSubRuleConfigs(
                                        createSubRuleConfig(
                                            "new-rule-1",
                                            AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR))
                                    .addSubRuleConfigs(
                                        createSubRuleConfig(
                                            "updated-rule-1",
                                            AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR))
                                    .build())
                            .build())
                    .build())
            .build();

    ScopedAnomalyConfigStatus status =
        createGlobalConfigStatus(
            RuleTestingMode.RULE_TESTING_MODE_ENABLED_FOR_NEW_AND_UPDATED_RULES);

    RulesChangeLog changelog =
        RulesChangeLog.newBuilder()
            .addRuleChanges(
                ThreatRuleChange.newBuilder()
                    .setRuleIdsAdded(StringList.newBuilder().addValues("new-rule-1").build())
                    .build())
            .addRuleChanges(
                ThreatRuleChange.newBuilder()
                    .setRuleUpdated(
                        ThreatRuleUpdateDetails.newBuilder()
                            .setRuleId("updated-rule-1")
                            .addUpdates(
                                ThreatRuleUpdateDetails.ThreatRuleUpdate.newBuilder()
                                    .setSignatureUpdated(true)
                                    .build())
                            .build())
                    .build())
            .build();

    when(ruleVersionManager.getRulesChangeLog(
            any(RuleType.class), any(RuleVersion.class), any(RuleVersion.class)))
        .thenReturn(changelog);

    Optional<ScopedAnomalyDetectionConfig> result =
        resolver.resolveGlobalTestingMode(inputConfig, Optional.of(status));

    assertTrue(result.isPresent());
    ModsecurityAnomalyRuleConfig ruleConfig =
        result
            .get()
            .getAnomalyDetectionConfigs(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule();

    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_TESTING,
        ruleConfig.getSubRuleConfigs(0).getAnomalyRuleAction());
    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_TESTING,
        ruleConfig.getSubRuleConfigs(1).getAnomalyRuleAction());
  }

  @Test
  void testResolveGlobalTestingMode_DisabledRuleNotChanged() {
    ScopedAnomalyDetectionConfig inputConfig =
        createScopedConfig("new-rule-1", AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE);
    ScopedAnomalyConfigStatus status =
        createGlobalConfigStatus(RuleTestingMode.RULE_TESTING_MODE_ENABLED_FOR_NEW_RULES);
    RulesChangeLog changelog =
        RulesChangeLog.newBuilder()
            .addRuleChanges(
                ThreatRuleChange.newBuilder()
                    .setRuleIdsAdded(StringList.newBuilder().addValues("new-rule-1").build())
                    .build())
            .build();

    when(ruleVersionManager.getRulesChangeLog(
            any(RuleType.class), any(RuleVersion.class), any(RuleVersion.class)))
        .thenReturn(changelog);

    Optional<ScopedAnomalyDetectionConfig> result =
        resolver.resolveGlobalTestingMode(inputConfig, Optional.of(status));

    assertTrue(result.isPresent());
    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE,
        result
            .get()
            .getAnomalyDetectionConfigs(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getSubRuleConfigs(0)
            .getAnomalyRuleAction());
  }

  @Test
  void testResolveGlobalTestingMode_InvalidRuleVersionData() {
    ScopedAnomalyDetectionConfig inputConfig =
        createScopedConfig("new-rule-1", AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR);
    ScopedAnomalyConfigStatus status =
        ScopedAnomalyConfigStatus.newBuilder()
            .setGlobalModsecConfig(
                GlobalModsecConfig.newBuilder()
                    .setRuleVersionData(
                        RuleVersionData.newBuilder()
                            .setRuleTestingMode(
                                RuleTestingMode.RULE_TESTING_MODE_ENABLED_FOR_NEW_RULES)
                            .build())
                    .build())
            .build();

    Optional<ScopedAnomalyDetectionConfig> result =
        resolver.resolveGlobalTestingMode(inputConfig, Optional.of(status));

    assertFalse(result.isPresent());
  }

  @Test
  void testResolveGlobalTestingMode_NonStableVersion() {
    ScopedAnomalyDetectionConfig inputConfig =
        createScopedConfig("new-rule-1", AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR);
    ScopedAnomalyConfigStatus status =
        ScopedAnomalyConfigStatus.newBuilder()
            .setGlobalModsecConfig(
                GlobalModsecConfig.newBuilder()
                    .setRuleVersionData(
                        RuleVersionData.newBuilder()
                            .setRuleTestingMode(
                                RuleTestingMode.RULE_TESTING_MODE_ENABLED_FOR_NEW_RULES)
                            .setCurrentVersion(
                                RuleVersion.newBuilder()
                                    .setPublishedDate("2023-07-02")
                                    .setVersionType(RuleVersionType.RULE_VERSION_TYPE_BETA)
                                    .build())
                            .setPreviousVersion(
                                RuleVersion.newBuilder().setPublishedDate("2023-07-01").build())
                            .build())
                    .build())
            .build();

    Optional<ScopedAnomalyDetectionConfig> result =
        resolver.resolveGlobalTestingMode(inputConfig, Optional.of(status));

    assertFalse(result.isPresent());
  }

  private ScopedAnomalyConfigStatus createGlobalConfigStatus(RuleTestingMode testingMode) {
    return ScopedAnomalyConfigStatus.newBuilder()
        .setGlobalModsecConfig(
            GlobalModsecConfig.newBuilder()
                .setRuleVersionData(
                    RuleVersionData.newBuilder()
                        .setRuleTestingMode(testingMode)
                        .setCurrentVersion(
                            RuleVersion.newBuilder()
                                .setPublishedDate("2023-07-02")
                                .setVersionType(RuleVersionType.RULE_VERSION_TYPE_STABLE)
                                .build())
                        .setPreviousVersion(
                            RuleVersion.newBuilder().setPublishedDate("2023-07-01").build())
                        .build())
                .build())
        .build();
  }

  private ScopedAnomalyDetectionConfig createScopedConfig(String ruleId, AnomalyRuleAction action) {
    return ScopedAnomalyDetectionConfig.newBuilder()
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setModsecurityAnomalyDetectionConfig(
                    ModsecurityAnomalyDetectionConfig.newBuilder()
                        .setModsecAnomalyRule(
                            ModsecurityAnomalyRuleConfig.newBuilder()
                                .addSubRuleConfigs(createSubRuleConfig(ruleId, action))
                                .build())
                        .build())
                .build())
        .build();
  }

  private AnomalySubRuleConfig createSubRuleConfig(String ruleId, AnomalyRuleAction action) {
    return AnomalySubRuleConfig.newBuilder()
        .setSubRuleId(ruleId)
        .setAnomalyRuleAction(action)
        .build();
  }
}
