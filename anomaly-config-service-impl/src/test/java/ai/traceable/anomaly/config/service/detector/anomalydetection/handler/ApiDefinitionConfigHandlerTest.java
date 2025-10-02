package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfigMap;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.JwtAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ApiDefinitionConfigHandlerTest {
  @Test
  void merge_prefersPreferredAndPopulatesNewFields() {
    ApiDefinitionRegistry registry = mock(ApiDefinitionRegistry.class);
    ApiDefinitionMetadataAnomalyDetectionConfig defaultCfg =
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("jwt")
            .setJwt(JwtAnomalyConfig.getDefaultInstance())
            .build();
    when(registry.getApiDefRuleIdToDetectionConfigMap())
        .thenReturn(Collections.singletonMap("jwt", defaultCfg));

    ApiDefinitionConfigHandler handler = new ApiDefinitionConfigHandler(registry);

    // MONITOR path
    ScopedAnomalyDetectionConfig preferred = getMonitorScopedWithApiDef("jwt_exp");
    ScopedAnomalyDetectionConfig fallback = getMonitorScopedWithApiDef("jwt_exp");

    List<AnomalyDetectionConfig> merged = handler.merge(preferred, fallback);

    assertEquals(1, merged.size());
    AnomalyDetectionConfig out = merged.get(0);
    assertTrue(out.hasApiDefinitionMetadataAnomalyDetectionConfig());
    assertEquals("jwt", out.getApiDefinitionMetadataAnomalyDetectionConfig().getAnomalyRuleId());

    AnomalySubRuleConfig sub =
        out.getApiDefinitionMetadataAnomalyDetectionConfig()
            .getSubRuleConfigs()
            .getSubRuleConfigsOrThrow("jwt_exp");
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR, sub.getAnomalyRuleAction());

    // DISABLE path (old fields -> action + internal)
    preferred = getDisabledScopedWithApiDef("jwt_exp");
    fallback = getDisabledScopedWithApiDef("jwt_exp");
    merged = handler.merge(preferred, fallback);

    assertEquals(1, merged.size());
    out = merged.get(0);
    assertTrue(out.hasApiDefinitionMetadataAnomalyDetectionConfig());
    assertEquals("jwt", out.getApiDefinitionMetadataAnomalyDetectionConfig().getAnomalyRuleId());

    sub =
        out.getApiDefinitionMetadataAnomalyDetectionConfig()
            .getSubRuleConfigs()
            .getSubRuleConfigsOrThrow("jwt_exp");
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE, sub.getAnomalyRuleAction());
    assertTrue(sub.getInternal());

    // TESTING path (new action + old fields preserved in mapping)
    preferred = getTestingScopedWithApiDef("jwt_exp");
    fallback = getTestingScopedWithApiDef("jwt_exp");
    merged = handler.merge(preferred, fallback);

    assertEquals(1, merged.size());
    out = merged.get(0);
    assertTrue(out.hasApiDefinitionMetadataAnomalyDetectionConfig());
    assertEquals("jwt", out.getApiDefinitionMetadataAnomalyDetectionConfig().getAnomalyRuleId());

    sub =
        out.getApiDefinitionMetadataAnomalyDetectionConfig()
            .getSubRuleConfigs()
            .getSubRuleConfigsOrThrow("jwt_exp");
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_TESTING, sub.getAnomalyRuleAction());
    assertFalse(sub.getInternal());
  }

  private ScopedAnomalyDetectionConfig getMonitorScopedWithApiDef(String... subRuleIds) {
    AnomalySubRuleConfigMap.Builder map = AnomalySubRuleConfigMap.newBuilder();
    AnomalySubRuleConfig.Builder monitoringConfig = AnomalySubRuleConfig.newBuilder();
    for (String id : subRuleIds) {
      map.putSubRuleConfigs(id, monitoringConfig.setSubRuleId(id).build());
    }
    ApiDefinitionMetadataAnomalyDetectionConfig apiDef =
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("jwt")
            .setJwt(JwtAnomalyConfig.getDefaultInstance())
            .setSubRuleConfigs(map)
            .build();
    return ScopedAnomalyDetectionConfig.newBuilder()
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setApiDefinitionMetadataAnomalyDetectionConfig(apiDef)
                .build())
        .build();
  }

  private ScopedAnomalyDetectionConfig getDisabledScopedWithApiDef(String... subRuleIds) {
    AnomalySubRuleConfigMap.Builder map = AnomalySubRuleConfigMap.newBuilder();
    AnomalySubRuleConfig.Builder disabledConfig =
        AnomalySubRuleConfig.newBuilder()
            .setConfigStatus(
                AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build());
    for (String id : subRuleIds) {
      map.putSubRuleConfigs(id, disabledConfig.setSubRuleId(id).build());
    }
    ApiDefinitionMetadataAnomalyDetectionConfig apiDef =
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("jwt")
            .setJwt(JwtAnomalyConfig.getDefaultInstance())
            .setSubRuleConfigs(map)
            .build();
    return ScopedAnomalyDetectionConfig.newBuilder()
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setApiDefinitionMetadataAnomalyDetectionConfig(apiDef)
                .build())
        .build();
  }

  private ScopedAnomalyDetectionConfig getTestingScopedWithApiDef(String... subRuleIds) {
    AnomalySubRuleConfigMap.Builder map = AnomalySubRuleConfigMap.newBuilder();
    AnomalySubRuleConfig.Builder testingConfig =
        AnomalySubRuleConfig.newBuilder()
            .setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_TESTING)
            .setConfigStatus(
                AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build());
    for (String id : subRuleIds) {
      map.putSubRuleConfigs(id, testingConfig.setSubRuleId(id).build());
    }
    ApiDefinitionMetadataAnomalyDetectionConfig apiDef =
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("jwt")
            .setJwt(JwtAnomalyConfig.getDefaultInstance())
            .setSubRuleConfigs(map)
            .build();
    return ScopedAnomalyDetectionConfig.newBuilder()
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setApiDefinitionMetadataAnomalyDetectionConfig(apiDef)
                .build())
        .build();
  }
}
