package ai.traceable.anomaly.config.service.exclusion.validators;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.exclusion.GetAnomalyExclusionRulesRequest;
import io.grpc.Status;
import jakarta.inject.Inject;

public class GetAnomalyExclusionRequestValidator
    implements RequestValidator<GetAnomalyExclusionRulesRequest> {
  private final AnomalyConfigValidator anomalyConfigValidator;

  @Inject
  GetAnomalyExclusionRequestValidator(AnomalyConfigValidator anomalyConfigValidator) {
    this.anomalyConfigValidator = anomalyConfigValidator;
  }

  public Status validate(GetAnomalyExclusionRulesRequest request) {
    if (request.getFilter().getRuleIdsList().stream().anyMatch(String::isEmpty)) {
      return Status.INVALID_ARGUMENT.withDescription("Rule Id can't be empty");
    }

    if (request.getFilter().getEventTypeIdsList().stream().anyMatch(String::isEmpty)) {
      return Status.INVALID_ARGUMENT.withDescription("Event Id can't be empty");
    }

    if (request.getFilter().getAnomalyActorIdsList().stream().anyMatch(String::isEmpty)) {
      return Status.INVALID_ARGUMENT.withDescription("Anomaly Actor Id can't be empty");
    }

    if (request.getFilter().getEventFamiliesList().stream()
        .anyMatch(
            family ->
                AnomalyEventFamily.UNRECOGNIZED.equals(family)
                    || AnomalyEventFamily.ANOMALY_EVENT_FAMILY_UNSPECIFIED.equals(family))) {
      return Status.INVALID_ARGUMENT.withDescription("Anomaly Event family not set");
    }

    if (request.getFilter().hasAnomalyConfigScope()) {
      Status configScopeStatus =
          anomalyConfigValidator.validate(request.getFilter().getAnomalyConfigScope());
      if (!configScopeStatus.isOk()) {
        return configScopeStatus;
      }
    }
    return Status.OK;
  }
}
