package ai.traceable.anomaly.config.service.exclusion.validators;

import ai.traceable.anomaly.config.service.v1.exclusion.UpdateAnomalyExclusionRuleRequest;
import io.grpc.Status;

public class UpdateAnomalyExclusionRequestValidator
    implements RequestValidator<UpdateAnomalyExclusionRuleRequest> {

  public Status validate(UpdateAnomalyExclusionRuleRequest request) {
    if (request.getRuleId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Rule Id required to update the rule");
    }

    if (request.getName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Rule name missing");
    }
    return Status.OK;
  }
}
