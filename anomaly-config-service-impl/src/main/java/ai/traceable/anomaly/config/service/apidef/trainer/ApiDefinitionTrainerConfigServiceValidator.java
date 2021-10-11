package ai.traceable.anomaly.config.service.apidef.trainer;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionApplierConfig;
import ai.traceable.anomaly.config.service.v1.apidef.GetApiDefinitionTrainerConfigsRequest;
import ai.traceable.anomaly.config.service.v1.apidef.UpdateApiDefinitionTrainerConfigsRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.EnumMap;
import java.util.List;

public class ApiDefinitionTrainerConfigServiceValidator {
  private final AnomalyConfigValidator anomalyConfigValidator;

  @Inject
  public ApiDefinitionTrainerConfigServiceValidator(AnomalyConfigValidator anomalyConfigValidator) {
    this.anomalyConfigValidator = anomalyConfigValidator;
  }

  public Status validate(GetApiDefinitionTrainerConfigsRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Api Definition Trainer Config Get request should have a valid config scope.");
    }
    return anomalyConfigValidator.validate(request.getConfigScope());
  }

  public Status validate(UpdateApiDefinitionTrainerConfigsRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Api Definition Trainer Config Update request should have a valid config scope.");
    }
    Status status = validate(request.getApiDefinitionApplierConfigsList());
    if (status == Status.OK) {
      return anomalyConfigValidator.validate(request.getConfigScope());
    }
    return status;
  }

  private Status validate(List<ApiDefinitionApplierConfig> applierConfigs) {
    EnumMap<ApiDefinitionApplierConfig.ApplierConfigCase, ApiDefinitionApplierConfig>
        applierConfigMap =
            new EnumMap<ApiDefinitionApplierConfig.ApplierConfigCase, ApiDefinitionApplierConfig>(
                ApiDefinitionApplierConfig.ApplierConfigCase.class);

    for (ApiDefinitionApplierConfig applierConfig : applierConfigs) {
      ApiDefinitionApplierConfig.ApplierConfigCase applierConfigCase =
          applierConfig.getApplierConfigCase();
      if (applierConfigMap.containsKey(applierConfigCase)) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Api Definition Trainer Config Update request should have only one applier config for applier: "
                + applierConfigCase);
      }
      applierConfigMap.put(applierConfigCase, applierConfig);
    }
    return Status.OK;
  }
}
