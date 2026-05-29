package ai.traceable.anomaly.config.service.registry.genai;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.detector.*;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GenAiRulesRegistryTest {

  @Test
  void testRuleIdToConfigMap() {
    GenAiRulesRegistryImpl volumetricRulesRegistry =
        new GenAiRulesRegistryImpl(new ConfigConverter());

    Map<String, GenAiAnomalyDetectionConfig> ruleIdToConfigMap =
        volumetricRulesRegistry.getGenAiRuleIdToConfigMap();

    Map<String, GenAiAnomalyDetectionConfig> expectedMap = new HashMap<>();
    expectedMap.put(
        "promptTextEvasionAndMisdirection",
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("promptTextEvasionAndMisdirection")
            .setPromptTextEvasionAndMisdirection(
                PromptTextEvasionAndMisdirectionAnomalyDetectionConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "promptInjection",
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("promptInjection")
            .setPromptInjection(PromptInjectionAnomalyDetectionConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "llmRateLimiting",
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("llmRateLimiting")
            .setLlmRateLimiting(LlmRateLimitingAnomalyDetectionConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "piiDetectedInPrompt",
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("piiDetectedInPrompt")
            .setPiiDetectedInPrompt(PiiDetectedInPromptAnomalyDetectionConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "llmInputExplosion",
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("llmInputExplosion")
            .setLlmInputExplosion(LlmInputExplosionAnomalyDetectionConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "llmModelGovernance",
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("llmModelGovernance")
            .setLlmModelGovernance(LlmModelGovernanceAnomalyDetectionConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "codeDetectedInPrompt",
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("codeDetectedInPrompt")
            .setCodeDetectedInPrompt(
                CodeDetectedInPromptAnomalyDetectionConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "toxicUnsafeContent",
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("toxicUnsafeContent")
            .setToxicUnsafeContent(ToxicUnsafeContentAnomalyDetectionConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "aiSensitiveDataProtection",
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("aiSensitiveDataProtection")
            .setAiSensitiveDataProtection(
                AiSensitiveDataProtectionAnomalyDetectionConfig.getDefaultInstance())
            .build());

    assertEquals(expectedMap, ruleIdToConfigMap);
  }
}
