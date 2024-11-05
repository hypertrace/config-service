package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfigMap;
import ai.traceable.anomaly.config.service.v1.detector.ApiCallSpikeAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiCallSpikeTuningConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiCallSpikeTuningConfigList;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.VolumetricAnomalyDetectionConfig;
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
}
