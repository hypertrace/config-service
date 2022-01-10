package ai.traceable.anomaly.config.service.detector;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import com.typesafe.config.Config;
import java.util.List;

public class DetectorConfigServiceConfig {
  private static final String MODSEC_DETECTION_CONFIGS_PATH = "modsecDetectionConfigs";
  private final List<AnomalyDetectionConfig> modsecDetectionConfigs;
  private final ConfigConverter configConverter = new ConfigConverter();

  public DetectorConfigServiceConfig(Config config) {
    this.modsecDetectionConfigs =
        configConverter.convertToAnomalyDetectionConfigs(
            config.getConfigList(MODSEC_DETECTION_CONFIGS_PATH));
  }

  public List<AnomalyDetectionConfig> getDefaultModsecDetectionConfigs() {
    return modsecDetectionConfigs;
  }
}
