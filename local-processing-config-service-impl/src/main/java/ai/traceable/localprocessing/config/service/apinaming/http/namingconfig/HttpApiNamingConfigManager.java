package ai.traceable.localprocessing.config.service.apinaming.http.namingconfig;

import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.HttpApiNamingConfigInfo;
import java.util.List;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRule;

public interface HttpApiNamingConfigManager {
  HttpApiNamingConfigInfo getHttpApiNamingConfigInfo(
      List<TrainingConfig> trainingConfigs, List<ApiNamingRule> apiNamingRules, String configHash);
}
