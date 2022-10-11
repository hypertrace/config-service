package ai.traceable.localprocessing.config.service.apinaming.http.namingconfig;

import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.HttpApiNamingConfigInfo;
import ai.traceable.span.processing.config.service.v1.ApiNamingRule;
import java.util.List;

public interface HttpApiNamingConfigManager {
  HttpApiNamingConfigInfo getHttpApiNamingConfigInfo(
      List<TrainingConfig> trainingConfigs, List<ApiNamingRule> apiNamingRules, String configHash);
}
