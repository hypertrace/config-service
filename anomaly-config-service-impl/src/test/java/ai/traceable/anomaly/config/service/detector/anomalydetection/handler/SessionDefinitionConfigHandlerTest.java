package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfigMap;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionViolationConfig;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class SessionDefinitionConfigHandlerTest {
  @Test
  void merge_prefersPreferredAndPopulatesNewFields() {
    SessionRulesRegistry registry = mock(SessionRulesRegistry.class);
    SessionDefinitionMetadataAnomalyDetectionConfig defaultCfg =
        SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("session_violation")
            .setSessionViolation(SessionViolationConfig.getDefaultInstance())
            .build();
    when(registry.getSessionDefRuleIdToDetectionConfigMap())
        .thenReturn(Collections.singletonMap("session_violation", defaultCfg));

    SessionDefinitionConfigHandler handler = new SessionDefinitionConfigHandler(registry);

    ScopedAnomalyDetectionConfig preferred =
        getMonitorScopedWithSessionDef("session_violation_basic");
    ScopedAnomalyDetectionConfig fallback =
        getMonitorScopedWithSessionDef("session_violation_basic");

    List<AnomalyDetectionConfig> merged = handler.merge(preferred, fallback);

    assertEquals(1, merged.size());
    AnomalyDetectionConfig out = merged.get(0);
    assertTrue(out.hasSessionDefinitionMetadataAnomalyDetectionConfig());
    assertEquals(
        "session_violation",
        out.getSessionDefinitionMetadataAnomalyDetectionConfig().getAnomalyRuleId());

    AnomalySubRuleConfig sub =
        out.getSessionDefinitionMetadataAnomalyDetectionConfig()
            .getSubRuleConfigs()
            .getSubRuleConfigsOrThrow("session_violation_basic");
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR, sub.getAnomalyRuleAction());

    preferred = getDisabledScopedWithSessionDef("session_violation_basic");
    fallback = getDisabledScopedWithSessionDef("session_violation_basic");

    merged = handler.merge(preferred, fallback);

    assertEquals(1, merged.size());
    out = merged.get(0);
    assertTrue(out.hasSessionDefinitionMetadataAnomalyDetectionConfig());
    assertEquals(
        "session_violation",
        out.getSessionDefinitionMetadataAnomalyDetectionConfig().getAnomalyRuleId());

    sub =
        out.getSessionDefinitionMetadataAnomalyDetectionConfig()
            .getSubRuleConfigs()
            .getSubRuleConfigsOrThrow("session_violation_basic");
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE, sub.getAnomalyRuleAction());
    assertTrue(sub.getInternal());

    preferred = getTestingScopedWithSessionDef("session_violation_basic");
    fallback = getTestingScopedWithSessionDef("session_violation_basic");
    merged = handler.merge(preferred, fallback);
    assertEquals(1, merged.size());
    out = merged.get(0);
    assertTrue(out.hasSessionDefinitionMetadataAnomalyDetectionConfig());
    assertEquals(
        "session_violation",
        out.getSessionDefinitionMetadataAnomalyDetectionConfig().getAnomalyRuleId());
    sub =
        out.getSessionDefinitionMetadataAnomalyDetectionConfig()
            .getSubRuleConfigs()
            .getSubRuleConfigsOrThrow("session_violation_basic");
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_TESTING, sub.getAnomalyRuleAction());
    assertFalse(sub.getInternal());
  }

  private ScopedAnomalyDetectionConfig getMonitorScopedWithSessionDef(String... subRuleIds) {
    AnomalySubRuleConfigMap.Builder map = AnomalySubRuleConfigMap.newBuilder();
    AnomalySubRuleConfig.Builder monitoringConfig = AnomalySubRuleConfig.newBuilder();
    for (String id : subRuleIds) {
      map.putSubRuleConfigs(id, monitoringConfig.setSubRuleId(id).build());
    }
    SessionDefinitionMetadataAnomalyDetectionConfig sessionDef =
        SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("session_violation")
            .setSessionViolation(SessionViolationConfig.getDefaultInstance())
            .setSubRuleConfigs(map)
            .build();
    return ScopedAnomalyDetectionConfig.newBuilder()
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setSessionDefinitionMetadataAnomalyDetectionConfig(sessionDef)
                .build())
        .build();
  }

  private ScopedAnomalyDetectionConfig getDisabledScopedWithSessionDef(String... subRuleIds) {
    AnomalySubRuleConfigMap.Builder map = AnomalySubRuleConfigMap.newBuilder();
    AnomalySubRuleConfig.Builder disabledConfig =
        AnomalySubRuleConfig.newBuilder()
            .setConfigStatus(
                AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build());
    for (String id : subRuleIds) {
      map.putSubRuleConfigs(id, disabledConfig.setSubRuleId(id).build());
    }
    SessionDefinitionMetadataAnomalyDetectionConfig sessionDef =
        SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("session_violation")
            .setSessionViolation(SessionViolationConfig.getDefaultInstance())
            .setSubRuleConfigs(map)
            .build();
    return ScopedAnomalyDetectionConfig.newBuilder()
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setSessionDefinitionMetadataAnomalyDetectionConfig(sessionDef)
                .build())
        .build();
  }

  private ScopedAnomalyDetectionConfig getTestingScopedWithSessionDef(String... subRuleIds) {
    AnomalySubRuleConfigMap.Builder map = AnomalySubRuleConfigMap.newBuilder();
    AnomalySubRuleConfig.Builder disabledConfig =
        AnomalySubRuleConfig.newBuilder()
            .setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_TESTING)
            .setConfigStatus(
                AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build());
    for (String id : subRuleIds) {
      map.putSubRuleConfigs(id, disabledConfig.setSubRuleId(id).build());
    }
    SessionDefinitionMetadataAnomalyDetectionConfig sessionDef =
        SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("session_violation")
            .setSessionViolation(SessionViolationConfig.getDefaultInstance())
            .setSubRuleConfigs(map)
            .build();
    return ScopedAnomalyDetectionConfig.newBuilder()
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setSessionDefinitionMetadataAnomalyDetectionConfig(sessionDef)
                .build())
        .build();
  }
}
