package ai.traceable.anomaly.config.service.registry.genai;

import ai.traceable.anomaly.config.service.v1.detector.GenAiAnomalyDetectionConfig;
import java.util.Map;

public interface GenAiRulesRegistry {

  Map<String, GenAiAnomalyDetectionConfig> getGenAiRuleIdToConfigMap();
}
