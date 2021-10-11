package ai.traceable.anomaly.config.service.apidef.trainer;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionApplierConfig;
import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionTrainerConfig;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ConfigManager {

  ApiDefinitionTrainerConfig getApiDefinitionTrainerConfig(
      RequestContext requestContext, AnomalyConfigScope configScope);

  ApiDefinitionTrainerConfig updateApiDefinitionTrainerConfig(
      RequestContext requestContext,
      List<ApiDefinitionApplierConfig> apiDefinitionApplierConfigs,
      AnomalyConfigScope configScope);
}
