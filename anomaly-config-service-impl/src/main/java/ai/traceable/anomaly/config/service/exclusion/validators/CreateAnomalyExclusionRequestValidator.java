package ai.traceable.anomaly.config.service.exclusion.validators;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope.ScopeCase;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActorExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActorExclusionInfo.AnomalyActorExclusionCase;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleData;
import ai.traceable.anomaly.config.service.v1.exclusion.CreateAnomalyExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionType;
import io.grpc.Status;
import jakarta.inject.Inject;

public class CreateAnomalyExclusionRequestValidator
    implements RequestValidator<CreateAnomalyExclusionRuleRequest> {
  private final AnomalyConfigValidator anomalyConfigValidator;

  @Inject
  CreateAnomalyExclusionRequestValidator(AnomalyConfigValidator anomalyConfigValidator) {
    this.anomalyConfigValidator = anomalyConfigValidator;
  }

  @Override
  public Status validate(CreateAnomalyExclusionRuleRequest request) {
    if (!request.hasRuleData()) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format("Rule data not found for request [%s]", request));
    }

    if (request.getRuleData().getName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Rule name missing");
    }

    AnomalyExclusionRuleData exclusionRuleData = request.getRuleData();
    if (!exclusionRuleData.hasAnomalyConfigScope()
        || !exclusionRuleData.hasEventExclusionInfo()
        || !exclusionRuleData.hasAnomalyActorExclusionInfo()) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "Missing data, unable to create exclusion rule for [%s]", exclusionRuleData));
    }

    if (ScopeCase.SCOPE_NOT_SET.equals(exclusionRuleData.getAnomalyConfigScope().getScopeCase())) {
      return Status.INVALID_ARGUMENT.withDescription("Exclusion config scope not defined");
    }
    Status configScopeStatus =
        anomalyConfigValidator.validate(exclusionRuleData.getAnomalyConfigScope());
    if (!configScopeStatus.isOk()) {
      return configScopeStatus;
    }

    EventExclusionInfo eventExclusionInfo = request.getRuleData().getEventExclusionInfo();
    if (!EventExclusionType.EVENT_EXCLUSION_TYPE_ALL_EVENTS.equals(
            eventExclusionInfo.getEventExclusionType())
        && (!eventExclusionInfo.hasEventTypeName()
            || !eventExclusionInfo.hasEventTypeId()
            || AnomalyEventFamily.ANOMALY_EVENT_FAMILY_UNSPECIFIED.equals(
                eventExclusionInfo.getAnomalyEventFamily()))) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid event exclusion info");
    }

    AnomalyActorExclusionInfo anomalyActorExclusionInfo =
        request.getRuleData().getAnomalyActorExclusionInfo();
    if (AnomalyActorExclusionCase.ANOMALY_ACTOR.equals(
            anomalyActorExclusionInfo.getAnomalyActorExclusionCase())
        && (!anomalyActorExclusionInfo.hasAnomalyActor()
            || anomalyActorExclusionInfo.getAnomalyActor().getId().isEmpty())) {
      return Status.INVALID_ARGUMENT.withDescription("missing anomaly actor id");
    }

    // Rule created for all APIs for the customer.
    if (ScopeCase.CUSTOMER_SCOPE.equals(exclusionRuleData.getAnomalyConfigScope().getScopeCase())
        && !EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_SUBTYPE.equals(
            eventExclusionInfo.getEventExclusionType())) {
      // Equivalent to disabling detection for customer.
      if (AnomalyActorExclusionCase.ANOMALYACTOREXCLUSION_NOT_SET.equals(
          exclusionRuleData.getAnomalyActorExclusionInfo().getAnomalyActorExclusionCase())) {
        return Status.UNIMPLEMENTED.withDescription(
            "Excluding all anomaly actors for all APIs is equivalent to disabling detection, operation not supported");
      }
    }
    return Status.OK;
  }
}
