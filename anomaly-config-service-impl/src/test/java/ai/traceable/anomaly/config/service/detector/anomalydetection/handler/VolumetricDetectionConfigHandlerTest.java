package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfigMap;
import ai.traceable.anomaly.config.service.v1.detector.ApiCallSpikeAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiCallSpikeTuningConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiCallSpikeTuningConfigList;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.VolumetricAnomalyDetectionConfig;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class VolumetricDetectionConfigHandlerTest {

  private VolumetricDetectionConfigHandler volumetricDetectionConfigHandler;

  @BeforeEach
  void setup() {
    volumetricDetectionConfigHandler =
        new VolumetricDetectionConfigHandler(mock(VolumetricRulesRegistry.class));
  }

  @Test
  void test_merge_config() {
    ScopedAnomalyDetectionConfig preferredConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setVolumetricAnomalyDetectionConfig(
                        VolumetricAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("volumetricApiCallSpike")
                            .setApiCallSpike(ApiCallSpikeAnomalyConfig.getDefaultInstance())
                            .build())
                    .build())
            .build();
    ScopedAnomalyDetectionConfig fallbackConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setVolumetricAnomalyDetectionConfig(
                        VolumetricAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("volumetric")
                            .setSubRuleConfigs(
                                AnomalySubRuleConfigMap.newBuilder()
                                    .putSubRuleConfigs(
                                        "volumetric_apiCallSpike",
                                        AnomalySubRuleConfig.newBuilder()
                                            .setSubRuleId("volumetric_apiCallSpike")
                                            .build()))
                            .setApiCallSpike(
                                ApiCallSpikeAnomalyConfig.newBuilder()
                                    .setApiCallSpikeTuningConfigList(
                                        ApiCallSpikeTuningConfigList.newBuilder()
                                            .addApiCallSpikeTuningConfigs(
                                                ApiCallSpikeTuningConfig.newBuilder()
                                                    .setEndpointSpanCountDetectionThreshold(100)
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    assertEquals(
        List.of(
            AnomalyDetectionConfig.newBuilder()
                .setVolumetricAnomalyDetectionConfig(
                    VolumetricAnomalyDetectionConfig.newBuilder()
                        .setAnomalyRuleId("volumetricApiCallSpike")
                        .setApiCallSpike(ApiCallSpikeAnomalyConfig.getDefaultInstance())
                        .build())
                .build(),
            AnomalyDetectionConfig.newBuilder()
                .setVolumetricAnomalyDetectionConfig(
                    VolumetricAnomalyDetectionConfig.newBuilder()
                        .setAnomalyRuleId("volumetric")
                        .setSubRuleConfigs(
                            AnomalySubRuleConfigMap.newBuilder()
                                .putSubRuleConfigs(
                                    "volumetric_apiCallSpike",
                                    AnomalySubRuleConfig.newBuilder()
                                        .setSubRuleId("volumetric_apiCallSpike")
                                        .setAnomalyRuleAction(
                                            AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR)
                                        .build()))
                        .setApiCallSpike(
                            ApiCallSpikeAnomalyConfig.newBuilder()
                                .setApiCallSpikeTuningConfigList(
                                    ApiCallSpikeTuningConfigList.newBuilder()
                                        .addApiCallSpikeTuningConfigs(
                                            ApiCallSpikeTuningConfig.newBuilder()
                                                .setEndpointSpanCountDetectionThreshold(100)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build()),
        volumetricDetectionConfigHandler.merge(preferredConfig, fallbackConfig));
  }

  @Test
  void merge_prefersPreferredAndPopulatesNewFields() {
    VolumetricRulesRegistry registry = mock(VolumetricRulesRegistry.class);
    VolumetricAnomalyDetectionConfig defaultCfg =
        VolumetricAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("volumetric")
            .setApiCallSpike(ApiCallSpikeAnomalyConfig.getDefaultInstance())
            .build();
    when(registry.getVolumetricRuleIdToConfigMap())
        .thenReturn(Collections.singletonMap("volumetric_apiCallSpike", defaultCfg));

    // MONITOR
    ScopedAnomalyDetectionConfig preferred =
        getMonitorScopedWithVolumetric("volumetric_apiCallSpike");
    ScopedAnomalyDetectionConfig fallback =
        getMonitorScopedWithVolumetric("volumetric_apiCallSpike");
    List<AnomalyDetectionConfig> merged =
        volumetricDetectionConfigHandler.merge(preferred, fallback);

    assertEquals(1, merged.size());
    AnomalyDetectionConfig out = merged.get(0);
    assertTrue(out.hasVolumetricAnomalyDetectionConfig());
    assertEquals("volumetric", out.getVolumetricAnomalyDetectionConfig().getAnomalyRuleId());

    AnomalySubRuleConfig sub =
        out.getVolumetricAnomalyDetectionConfig()
            .getSubRuleConfigs()
            .getSubRuleConfigsOrThrow("volumetric_apiCallSpike");
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR, sub.getAnomalyRuleAction());

    // DISABLE (old fields)
    preferred = getDisabledScopedWithVolumetric("volumetric_apiCallSpike");
    fallback = getDisabledScopedWithVolumetric("volumetric_apiCallSpike");
    merged = volumetricDetectionConfigHandler.merge(preferred, fallback);

    assertEquals(1, merged.size());
    out = merged.get(0);
    assertTrue(out.hasVolumetricAnomalyDetectionConfig());
    assertEquals("volumetric", out.getVolumetricAnomalyDetectionConfig().getAnomalyRuleId());

    sub =
        out.getVolumetricAnomalyDetectionConfig()
            .getSubRuleConfigs()
            .getSubRuleConfigsOrThrow("volumetric_apiCallSpike");
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE, sub.getAnomalyRuleAction());
    assertTrue(sub.getInternal());

    // TESTING (new action + old fields mapping retained)
    preferred = getTestingScopedWithVolumetric("volumetric_apiCallSpike");
    fallback = getTestingScopedWithVolumetric("volumetric_apiCallSpike");
    merged = volumetricDetectionConfigHandler.merge(preferred, fallback);

    assertEquals(1, merged.size());
    out = merged.get(0);
    assertTrue(out.hasVolumetricAnomalyDetectionConfig());
    assertEquals("volumetric", out.getVolumetricAnomalyDetectionConfig().getAnomalyRuleId());

    sub =
        out.getVolumetricAnomalyDetectionConfig()
            .getSubRuleConfigs()
            .getSubRuleConfigsOrThrow("volumetric_apiCallSpike");
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_TESTING, sub.getAnomalyRuleAction());
    assertFalse(sub.getInternal());
  }

  private ScopedAnomalyDetectionConfig getMonitorScopedWithVolumetric(String... subRuleIds) {
    AnomalySubRuleConfigMap.Builder map = AnomalySubRuleConfigMap.newBuilder();
    AnomalySubRuleConfig.Builder monitoringConfig = AnomalySubRuleConfig.newBuilder();
    for (String id : subRuleIds) {
      map.putSubRuleConfigs(id, monitoringConfig.setSubRuleId(id).build());
    }
    VolumetricAnomalyDetectionConfig volumetric =
        VolumetricAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("volumetric")
            .setApiCallSpike(ApiCallSpikeAnomalyConfig.getDefaultInstance())
            .setSubRuleConfigs(map)
            .build();
    return ScopedAnomalyDetectionConfig.newBuilder()
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setVolumetricAnomalyDetectionConfig(volumetric)
                .build())
        .build();
  }

  private ScopedAnomalyDetectionConfig getDisabledScopedWithVolumetric(String... subRuleIds) {
    AnomalySubRuleConfigMap.Builder map = AnomalySubRuleConfigMap.newBuilder();
    AnomalySubRuleConfig.Builder disabledConfig =
        AnomalySubRuleConfig.newBuilder()
            .setConfigStatus(
                AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build());
    for (String id : subRuleIds) {
      map.putSubRuleConfigs(id, disabledConfig.setSubRuleId(id).build());
    }
    VolumetricAnomalyDetectionConfig volumetric =
        VolumetricAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("volumetric")
            .setApiCallSpike(ApiCallSpikeAnomalyConfig.getDefaultInstance())
            .setSubRuleConfigs(map)
            .build();
    return ScopedAnomalyDetectionConfig.newBuilder()
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setVolumetricAnomalyDetectionConfig(volumetric)
                .build())
        .build();
  }

  private ScopedAnomalyDetectionConfig getTestingScopedWithVolumetric(String... subRuleIds) {
    AnomalySubRuleConfigMap.Builder map = AnomalySubRuleConfigMap.newBuilder();
    AnomalySubRuleConfig.Builder testingConfig =
        AnomalySubRuleConfig.newBuilder()
            .setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_TESTING)
            .setConfigStatus(
                AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build());
    for (String id : subRuleIds) {
      map.putSubRuleConfigs(id, testingConfig.setSubRuleId(id).build());
    }
    VolumetricAnomalyDetectionConfig volumetric =
        VolumetricAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("volumetric")
            .setApiCallSpike(ApiCallSpikeAnomalyConfig.getDefaultInstance())
            .setSubRuleConfigs(map)
            .build();
    return ScopedAnomalyDetectionConfig.newBuilder()
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setVolumetricAnomalyDetectionConfig(volumetric)
                .build())
        .build();
  }
}
