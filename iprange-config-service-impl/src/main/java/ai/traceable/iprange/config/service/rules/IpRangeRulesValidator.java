package ai.traceable.iprange.config.service.rules;

import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_BLOCK_ALL_EXCEPT;

import ai.traceable.config.utils.RegexValidator;
import ai.traceable.iprange.config.service.v1.AgentModification;
import ai.traceable.iprange.config.service.v1.CreateIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.DeleteIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.EventSeverity;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import ai.traceable.iprange.config.service.v1.IpRangeRuleDetails;
import ai.traceable.iprange.config.service.v1.RuleAction;
import ai.traceable.iprange.config.service.v1.RuleScope;
import ai.traceable.iprange.config.service.v1.StatusCodeMatchCondition;
import ai.traceable.iprange.config.service.v1.StatusCodeMatchType;
import ai.traceable.iprange.config.service.v1.UpdateIpRangeRuleRequest;
import ai.traceable.platform.utils.ip.IpValidationUtils;
import io.grpc.Status;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;

class IpRangeRulesValidator implements RulesValidator {

  @Override
  public Status validate(
      CreateIpRangeRuleRequest request, Supplier<List<IpRangeRule>> ipRangeRulesSupplier) {
    Status status = validate(request.getRuleDetails());
    if (!status.isOk()) {
      return status;
    }
    status = validate(request.getRuleScope());
    if (!status.isOk()) {
      return status;
    }
    if (RULE_ACTION_BLOCK_ALL_EXCEPT.equals(request.getRuleDetails().getRuleAction())
        && isDuplicateBlockAllExceptCreate(
            request.getRuleScope().getEnvironmentScope().getEnvironmentIdsList(),
            ipRangeRulesSupplier)) {
      return Status.ALREADY_EXISTS.withDescription(
          "Trying to create duplicate rule for action " + RULE_ACTION_BLOCK_ALL_EXCEPT);
    }
    Optional<IpRangeRule> ipRangeRuleWithSameName =
        getIpRangeRuleWithSameName(request.getRuleDetails().getName(), ipRangeRulesSupplier);
    if (ipRangeRuleWithSameName.isPresent()) {
      return Status.ALREADY_EXISTS.withDescription(
          String.format(
              "Ip range rule with name : {} already exist", request.getRuleDetails().getName()));
    }
    return Status.OK;
  }

  @Override
  public Status validate(
      UpdateIpRangeRuleRequest request, Supplier<List<IpRangeRule>> ipRangeRulesSupplier) {
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
    if (RULE_ACTION_BLOCK_ALL_EXCEPT.equals(request.getRuleDetails().getRuleAction())
        && isDuplicateBlockAllExceptUpdate(
            request.getId(),
            request.getRuleScope().getEnvironmentScope().getEnvironmentIdsList(),
            ipRangeRulesSupplier)) {
      return Status.ALREADY_EXISTS.withDescription(
          "Trying to change rule action to "
              + RULE_ACTION_BLOCK_ALL_EXCEPT
              + ". Rule of this type already exists");
    }
    Optional<IpRangeRule> ipRangeRuleWithSameName =
        getIpRangeRuleWithSameName(request.getRuleDetails().getName(), ipRangeRulesSupplier);
    if (ipRangeRuleWithSameName.isPresent()
        && !ipRangeRuleWithSameName.get().getId().equals(request.getId())) {
      return Status.ALREADY_EXISTS.withDescription(
          String.format(
              "Ip range rule with name : {} already exist", request.getRuleDetails().getName()));
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
    } else {
      // validate ranges
      Status ipValidationStatus = validateRawInputIpDataList(details);
      if (!ipValidationStatus.isOk()) {
        return ipValidationStatus;
      }
    }

    if (details.getRuleAction() == RuleAction.RULE_ACTION_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "IP Range rule should have a valid action type");
    }

    if ((details.getRuleAction() == RuleAction.RULE_ACTION_BLOCK_ALL_EXCEPT
            || details.getRuleAction() == RuleAction.RULE_ACTION_BLOCK)
        && details.getEffectsList().stream()
            .flatMap(effect -> effect.getAgentRuleEffect().getAgentModificationsList().stream())
            .anyMatch(AgentModification::hasHeaderInjection)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "IP Range rule with BLOCK action should not have header injection");
    }

    if (details.getRuleAction() == RuleAction.RULE_ACTION_ALERT
        && details.getEventSeverity() == EventSeverity.EVENT_SEVERITY_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "IP Range rule with ALERT action should have a valid severity");
    }

    if (details.getExpirationDetails().hasExpirationDuration()) {
      try {
        java.time.Duration.parse(details.getExpirationDetails().getExpirationDuration());
      } catch (DateTimeParseException iae) {
        return Status.INVALID_ARGUMENT.withDescription(
            "IP Range rule should have a valid expiration duration in ISO 8601 format");
      }
    }

    StatusCodeMatchCondition statusCodeMatchCondition =
        details.getConditions().getStatusCodeMatchCondition();
    if (statusCodeMatchCondition.getMatchType()
            == StatusCodeMatchType.STATUS_CODE_MATCH_TYPE_MATCHES_REGEX
        || statusCodeMatchCondition.getMatchType()
            == StatusCodeMatchType.STATUS_CODE_MATCH_TYPE_NOT_MATCH_REGEX) {
      return RegexValidator.validateRegex(statusCodeMatchCondition.getMatchValue());
    }
    return Status.OK;
  }

  private Status validate(RuleScope scope) {
    if (scope.hasEnvironmentScope()) {
      List<String> environmentIdList = scope.getEnvironmentScope().getEnvironmentIdsList();
      if (environmentIdList.isEmpty()) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Environment scope should have at least one environment");
      } else {
        return environmentIdList.stream().anyMatch(String::isEmpty)
            ? Status.INVALID_ARGUMENT.withDescription("Environment id should not be empty string.")
            : Status.OK;
      }
    } else {
      return Status.OK;
    }
  }

  private Status validateRawInputIpDataList(IpRangeRuleDetails details) {
    return details.getRawInputIpDataList().stream()
        .filter(Predicate.not(this::validateRawInputIpOrThrow))
        .findAny()
        .map(
            ipAddress ->
                Status.INVALID_ARGUMENT.withDescription("Found invalid IP/CIDR " + ipAddress))
        .orElse(Status.OK);
  }

  private boolean isDuplicateBlockAllExceptCreate(
      List<String> environmentIds, Supplier<List<IpRangeRule>> ipRangeRulesSupplier) {
    return ipRangeRulesSupplier.get().stream()
        .anyMatch(rule -> isDuplicateBlockAllExceptRule(rule, environmentIds));
  }

  private boolean isDuplicateBlockAllExceptUpdate(
      String id, List<String> environmentIds, Supplier<List<IpRangeRule>> ipRangeRulesSupplier) {
    return ipRangeRulesSupplier.get().stream()
        .anyMatch(
            rule ->
                !rule.getId().equals(id) && isDuplicateBlockAllExceptRule(rule, environmentIds));
  }

  private boolean isDuplicateBlockAllExceptRule(IpRangeRule rule, List<String> environmentIds) {
    List<String> ruleEnvironmentIds =
        rule.getRuleScope().getEnvironmentScope().getEnvironmentIdsList();
    return rule.getRuleDetails().getRuleAction().equals(RULE_ACTION_BLOCK_ALL_EXCEPT)
        && ( // Not allowing all environments block all except in case a rule already exists
        environmentIds.isEmpty()
            || // Not allowing any block all except rule if already an all environments one exists
            ruleEnvironmentIds.isEmpty()
            || // Only allowing another block all except rule if it has no environment intersection
            // with an existing rule
            ruleEnvironmentIds.stream().anyMatch(environmentIds::contains));
  }

  private boolean validateRawInputIpOrThrow(String rawInputIp) {
    return IpValidationUtils.isIpAddressRangeInCIDRWithHostBitsZero(rawInputIp);
  }

  private static Optional<IpRangeRule> getIpRangeRuleWithSameName(
      String name, Supplier<List<IpRangeRule>> existingIpRangeRulesSupplier) {
    return existingIpRangeRulesSupplier.get().stream()
        .filter(rule -> rule.getRuleDetails().getName().equals(name))
        .findFirst();
  }
}
