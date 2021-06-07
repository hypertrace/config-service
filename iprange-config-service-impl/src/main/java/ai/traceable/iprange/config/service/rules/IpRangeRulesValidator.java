package ai.traceable.iprange.config.service.rules;

import ai.traceable.iprange.config.service.v1.CreateIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.DeleteIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.IpRangeRuleDetails;
import ai.traceable.iprange.config.service.v1.RuleAction;
import ai.traceable.iprange.config.service.v1.UpdateIpRangeRuleRequest;
import io.grpc.Status;
import java.time.format.DateTimeParseException;

class IpRangeRulesValidator implements RulesValidator {

  @Override
  public Status validate(CreateIpRangeRuleRequest request) {
    return validate(request.getRuleDetails());
  }

  @Override
  public Status validate(UpdateIpRangeRuleRequest request) {
    if (request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Update Ip Range rule should have a valid id");
    }
    return validate(request.getRuleDetails());
  }

  @Override
  public Status validate(DeleteIpRangeRuleRequest request) {
    if (request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Delete Ip Range rule should have a valid id");
    }
    return Status.OK;
  }

  private Status validate(IpRangeRuleDetails details) {
    if (details.getName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("IP Range rule should have a valid name");
    }

    if (details.getRawInputIpDataList().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "IP Range rule should have at least one valid IP address or range");
    }

    if (details.getRuleAction() == RuleAction.RULE_ACTION_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "IP Range rule should have a valid action type");
    }

    if (details.getExpirationDetails().hasExpirationDuration()) {
      try {
        java.time.Duration.parse(details.getExpirationDetails().getExpirationDuration());
      } catch (DateTimeParseException iae) {
        return Status.INVALID_ARGUMENT.withDescription(
            "IP Range rule should have a valid expiration duration in ISO 8601 format");
      }
    }
    return Status.OK;
  }
}
