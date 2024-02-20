package ai.traceable.anomaly.config.service.registry.volumetric;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.detector.VolumetricAnomalyDetectionConfig;
import java.util.Map;

public interface VolumetricRulesRegistry {

  Map<String, AnomalyRuleInfo> getVolumetricRuleInfos();

  Map<String, VolumetricAnomalyDetectionConfig> getVolumetricRuleIdToConfigMap();
}
