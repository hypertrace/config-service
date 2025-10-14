package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.registry.genai.GenAiRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.CodeDetectedInPromptAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.CodeDetectedInPromptThreatRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.GenAiAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GenAiConfigHandlerTest {

  @Test
  void test() {
    GenAiRulesRegistry genAiRulesRegistry = mock(GenAiRulesRegistry.class);
    when(genAiRulesRegistry.getGenAiRuleIdToConfigMap())
        .thenReturn(
            Map.of(
                "codeDetectedInPrompt",
                GenAiAnomalyDetectionConfig.newBuilder()
                    .setAnomalyRuleId("codeDetectedInPrompt")
                    .setCodeDetectedInPrompt(
                        CodeDetectedInPromptAnomalyDetectionConfig.getDefaultInstance())
                    .build()));
    GenAiDetectionConfigHandler handler = new GenAiDetectionConfigHandler(genAiRulesRegistry);
    ScopedAnomalyDetectionConfig pref =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(true))
                    .setGenAiAnomalyDetectionConfig(
                        GenAiAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("codeDetectedInPrompt")))
            .build();

    ScopedAnomalyDetectionConfig fall =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setGenAiAnomalyDetectionConfig(
                        GenAiAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("codeDetectedInPrompt")
                            .setCodeDetectedInPrompt(
                                CodeDetectedInPromptAnomalyDetectionConfig.newBuilder()
                                    .addThreatRuleConfigs(
                                        CodeDetectedInPromptThreatRuleConfig.newBuilder()
                                            .setThreatRuleId("rule1")
                                            .setSubRuleIds(
                                                StringList.newBuilder()
                                                    .addValues("subRule1")
                                                    .addValues("subRule2"))))))
            .build();

    List<AnomalyDetectionConfig> detectionConfigs = handler.merge(pref, fall);

    AnomalyDetectionConfig detectionConfig = detectionConfigs.get(0);

    assertTrue(detectionConfig.getConfigStatus().getDisabled());
    assertEquals(
        "rule1",
        detectionConfig
            .getGenAiAnomalyDetectionConfig()
            .getCodeDetectedInPrompt()
            .getThreatRuleConfigs(0)
            .getThreatRuleId());
    assertEquals(
        StringList.newBuilder().addValues("subRule1").addValues("subRule2").build(),
        detectionConfig
            .getGenAiAnomalyDetectionConfig()
            .getCodeDetectedInPrompt()
            .getThreatRuleConfigs(0)
            .getSubRuleIds());
  }
}
