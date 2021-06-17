package ai.traceable.anomaly.config.service.exclusion.validators;

import ai.traceable.anomaly.config.service.v1.exclusion.DeleteAnomalyExclusionRuleRequest;
import io.grpc.Status;

public class DeleteExclusionRuleRequestValidator
    implements RequestValidator<DeleteAnomalyExclusionRuleRequest> {

  @Override
  public Status validate(DeleteAnomalyExclusionRuleRequest request) {
    if (request.getRuleId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "RuleId required to delete the exclusion rule");
    }
    return Status.OK;
  }
}
