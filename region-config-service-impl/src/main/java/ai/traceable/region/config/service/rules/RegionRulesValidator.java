package ai.traceable.region.config.service.rules;

import ai.traceable.region.config.service.v1.AgentModification;
import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.DeleteRegionRuleRequest;
import ai.traceable.region.config.service.v1.EventSeverity;
import ai.traceable.region.config.service.v1.GetRegionRequest;
import ai.traceable.region.config.service.v1.RegionIdentifier;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import ai.traceable.region.config.service.v1.RegionsFilter;
import ai.traceable.region.config.service.v1.RuleScope;
import ai.traceable.region.config.service.v1.UpdateRegionRuleRequest;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

class RegionRulesValidator implements RulesValidator {

  @Override
  public Status validate(
      CreateRegionRuleRequest request, Supplier<List<RegionRule>> existingRegionRulesSupplier) {
    Status status = validate(request.getRuleScope());
    if (!status.isOk()) {
      return status;
    }
    if (request.getRegionIdList().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "create region rule should have non empty region id list");
    }

    if (request.getName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("create region rule should have a valid name");
    }

    if (request.getActionType() == RegionRuleActionType.REGION_RULE_ACTION_TYPE_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "create region rule should have a valid action type");
    }

    if ((request.getActionType().equals(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
            || request
                .getActionType()
                .equals(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT))
        && request.getEffectsList().stream()
            .flatMap(effect -> effect.getAgentRuleEffect().getAgentModificationsList().stream())
            .anyMatch(AgentModification::hasHeaderInjection)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "create region rule with block action should not have header injection");
    }

    if (request.getActionType() == RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALERT
        && request.getEventSeverity() == EventSeverity.EVENT_SEVERITY_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "create region rule with alert action should have a valid severity");
    }
    if (RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT.equals(
            request.getActionType())
        && isDuplicateBlockAllExceptRuleCreate(
            request.getRuleScope().getEnvironmentScope().getEnvironmentIdsList(),
            existingRegionRulesSupplier)) {
      return Status.ALREADY_EXISTS.withDescription(
          "Trying to create duplicate rule for action "
              + RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT);
    }
    Optional<RegionRule> ruleWithSameName =
        getRegionRuleWithSameName(request.getName(), existingRegionRulesSupplier);
    if (ruleWithSameName.isPresent()) {
      return Status.ALREADY_EXISTS.withDescription(
          String.format("Region rule with name : {} already exist", request.getName()));
    }
    return Status.OK;
  }

  @Override
  public Status validate(
      UpdateRegionRuleRequest request, Supplier<List<RegionRule>> existingRegionRulesSupplier) {
    Status status = validate(request.getRuleScope());
    if (!status.isOk()) {
      return status;
    }
    if (request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("update region rule should have a valid id");
    }

    if (request.getRegionIdList().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "update region rule should have non empty region id list");
    }

    if (request.getName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("update region rule should have a valid name");
    }

    if (request.getActionType() == RegionRuleActionType.REGION_RULE_ACTION_TYPE_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "update region rule should have a valid action type");
    }

    if ((request.getActionType().equals(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
            || request
                .getActionType()
                .equals(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT))
        && request.getEffectsList().stream()
            .flatMap(effect -> effect.getAgentRuleEffect().getAgentModificationsList().stream())
            .anyMatch(AgentModification::hasHeaderInjection)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "update region rule with block action should not have header injection");
    }

    if (request.getActionType() == RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALERT
        && request.getEventSeverity() == EventSeverity.EVENT_SEVERITY_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "update region rule with alert action should have a valid severity");
    }

    if (RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT.equals(
            request.getActionType())
        && isDuplicateBlockAllExceptRuleUpdate(
            request.getId(),
            request.getRuleScope().getEnvironmentScope().getEnvironmentIdsList(),
            existingRegionRulesSupplier)) {
      return Status.ALREADY_EXISTS.withDescription(
          "Trying to change rule action to "
              + RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT
              + ". Rule of this type already exists.");
    }
    Optional<RegionRule> ruleWithSameName =
        getRegionRuleWithSameName(request.getName(), existingRegionRulesSupplier);
    if (ruleWithSameName.isPresent() && !ruleWithSameName.get().getId().equals(request.getId())) {
      return Status.ALREADY_EXISTS.withDescription(
          String.format("Region rule with name : {} already exist", request.getName()));
    }
    return Status.OK;
  }

  @Override
  public Status validate(DeleteRegionRuleRequest request) {
    String ruleId = request.getId();
    if (ruleId.isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("delete region rule should have a valid id");
    }

    return Status.OK;
  }

  @Override
  public Status validate(RegionsFilter filter) {
    if (!filter.getIdList().isEmpty() && !filter.getRegionIdentifierList().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Both id and region identifier should not be present.");
    }

    return filter.getRegionIdentifierList().stream()
        .map(this::validate)
        .filter(status -> !status.isOk())
        .findFirst()
        .orElse(Status.OK);
  }

  @Override
  public Status validate(GetRegionRequest request) {
    if (request.hasRegionIdentifier() && !request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Both id and region identifier should not be present.");
    } else if (!request.hasRegionIdentifier() && request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Both id and region identifier should not be absent.");
    } else if (request.hasRegionIdentifier()) {
      return validate(request.getRegionIdentifier());
    } else {
      return Status.OK;
    }
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

  private boolean isDuplicateBlockAllExceptRuleCreate(
      List<String> environmentIds, Supplier<List<RegionRule>> existingRegionRulesSupplier) {
    return existingRegionRulesSupplier.get().stream()
        .anyMatch(regionRule -> isDuplicateBlockAllExceptRule(regionRule, environmentIds));
  }

  private boolean isDuplicateBlockAllExceptRuleUpdate(
      String id,
      List<String> environmentIds,
      Supplier<List<RegionRule>> existingRegionRulesSupplier) {
    return existingRegionRulesSupplier.get().stream()
        .anyMatch(
            regionRule ->
                !id.equals(regionRule.getId())
                    && isDuplicateBlockAllExceptRule(regionRule, environmentIds));
  }

  private boolean isDuplicateBlockAllExceptRule(RegionRule rule, List<String> environmentIds) {
    List<String> ruleEnvironmentIds =
        rule.getRuleScope().getEnvironmentScope().getEnvironmentIdsList();
    return rule.getActionType()
            .equals(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
        && ( // Not allowing all environments block all except in case a rule already exists
        environmentIds.isEmpty()
            || // Not allowing any block all except rule if already an all environments one exists
            ruleEnvironmentIds.isEmpty()
            || // Only allowing another block all except rule if it has no environment intersection
            // with an existing rule
            ruleEnvironmentIds.stream().anyMatch(environmentIds::contains));
  }

  private Status validate(RegionIdentifier identifier) {
    switch (identifier.getIdentifierCase()) {
      case COUNTRY_ISO_CODE:
        return identifier.getCountryIsoCode().isEmpty()
            ? Status.INVALID_ARGUMENT.withDescription("Country iso code cannot be empty")
            : Status.OK;
      default:
        return Status.INVALID_ARGUMENT.withDescription(
            String.format(
                "Invalid case: %s found for region identifier.", identifier.getIdentifierCase()));
    }
  }

  private static Optional<RegionRule> getRegionRuleWithSameName(
      String name, Supplier<List<RegionRule>> existingRegionRulesSupplier) {
    return existingRegionRulesSupplier.get().stream()
        .filter(rule -> rule.getName().equals(name))
        .findFirst();
  }
}
