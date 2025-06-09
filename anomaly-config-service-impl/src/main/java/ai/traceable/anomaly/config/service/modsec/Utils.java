package ai.traceable.anomaly.config.service.modsec;

import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Utils {

  public static Map<String, AnomalySubRuleConfig> getSubRuleConfigMap(
      AnomalyDetectionConfig detectionConfig) {
    return detectionConfig
        .getModsecurityAnomalyDetectionConfig()
        .getModsecAnomalyRule()
        .getSubRuleConfigsList()
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(AnomalySubRuleConfig::getSubRuleId, Function.identity()));
  }

  public static Map<String, AnomalyDetectionConfig> getModsecAnomalyRuleConfigMap(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig) {
    return scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList().stream()
        .filter(
            detectionConfig ->
                detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAnomalyRule())
        .collect(
            Collectors.toUnmodifiableMap(
                detectionConfig ->
                    detectionConfig
                        .getModsecurityAnomalyDetectionConfig()
                        .getModsecAnomalyRule()
                        .getAnomalyRuleId(),
                Function.identity()));
  }
}
