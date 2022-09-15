package ai.traceable.iprange.config.service.rules;

import ai.traceable.iprange.config.service.v1.CreateIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.DeleteIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import ai.traceable.iprange.config.service.v1.IpRangeRuleDetails;
import ai.traceable.iprange.config.service.v1.RuleAction;
import ai.traceable.iprange.config.service.v1.RuleScope;
import ai.traceable.iprange.config.service.v1.UpdateIpRangeRuleRequest;
import io.grpc.Status;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.function.Supplier;

class IpRangeRulesValidator implements RulesValidator {

  @Override
  public Status validate(
      CreateIpRangeRuleRequest request, Supplier<List<IpRangeRule>> blockAllExceptRulesSupplier) {
    Status status = validate(request.getRuleDetails());
    if (!status.isOk()) {
      return status;
    }
    status = validate(request.getRuleScope());
    if (!status.isOk()) {
      return status;
    }
    if (RuleAction.RULE_ACTION_BLOCK_ALL_EXCEPT.equals(request.getRuleDetails().getRuleAction())
        && isDuplicateBlockAllExceptCreate(blockAllExceptRulesSupplier)) {
      return Status.ALREADY_EXISTS.withDescription(
          "Trying to create duplicate rule for action " + RuleAction.RULE_ACTION_BLOCK_ALL_EXCEPT);
    }
    return Status.OK;
  }

  @Override
  public Status validate(
      UpdateIpRangeRuleRequest request, Supplier<List<IpRangeRule>> blockAllExceptRulesSupplier) {
    if (request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Update Ip Range rule should have a valid id");
    }
    Status status = validate(request.getRuleDetails());
    if (!status.isOk()) {
      return status;
    }
    status = validate(request.getRuleScope());
    if (!status.isOk()) {
      return status;
    }
    if (RuleAction.RULE_ACTION_BLOCK_ALL_EXCEPT.equals(request.getRuleDetails().getRuleAction())
        && isDuplicateBlockAllExceptUpdate(request.getId(), blockAllExceptRulesSupplier)) {
      return Status.ALREADY_EXISTS.withDescription(
          "Trying to change rule action to "
              + RuleAction.RULE_ACTION_BLOCK_ALL_EXCEPT
              + ". Rule of this type already exists");
    }
    return Status.OK;
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

  private Status validate(RuleScope ruleScope) {
    if (ruleScope.hasEnvironmentScope()
        && ruleScope.getEnvironmentScope().getEnvironmentIdsCount() == 0) {
      return Status.INVALID_ARGUMENT.withDescription(
          "EnvironmentScope should have non empty id list");
    }
    return Status.OK;
  }

  private boolean isDuplicateBlockAllExceptCreate(
      Supplier<List<IpRangeRule>> blockAllExceptRulesSupplier) {
    return !blockAllExceptRulesSupplier.get().isEmpty();
  }

  private boolean isDuplicateBlockAllExceptUpdate(
      String id, Supplier<List<IpRangeRule>> blockAllExceptRulesSupplier) {
    List<IpRangeRule> ipRangeRules = blockAllExceptRulesSupplier.get();
    return ipRangeRules.stream().anyMatch(rule -> !rule.getId().equals(id));
  }
}
