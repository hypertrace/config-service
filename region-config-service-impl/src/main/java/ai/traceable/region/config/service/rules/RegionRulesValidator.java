package ai.traceable.region.config.service.rules;

import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.DeleteRegionRuleRequest;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import ai.traceable.region.config.service.v1.RuleScope;
import ai.traceable.region.config.service.v1.UpdateRegionRuleRequest;
import io.grpc.Status;
import java.util.List;
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

    if (RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT.equals(
            request.getActionType())
        && isDuplicateBlockAllExceptRuleCreate(existingRegionRulesSupplier)) {
      return Status.ALREADY_EXISTS.withDescription(
          "Trying to create duplicate rule for action "
              + RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT);
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

    if (RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT.equals(
            request.getActionType())
        && isDuplicateBlockAllExceptRuleUpdate(request.getId(), existingRegionRulesSupplier)) {
      return Status.ALREADY_EXISTS.withDescription(
          "Trying to change rule action to "
              + RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT
              + ". Rule of this type already exists.");
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

  private Status validate(RuleScope ruleScope) {
    if (ruleScope.hasEnvironmentScope()
        && ruleScope.getEnvironmentScope().getEnvironmentIdsCount() == 0) {
      return Status.INVALID_ARGUMENT.withDescription(
          "environment scope should have non empty id list");
    }
    return Status.OK;
  }

  private boolean isDuplicateBlockAllExceptRuleCreate(
      Supplier<List<RegionRule>> existingRegionRulesSupplier) {
    return existingRegionRulesSupplier.get().stream()
        .anyMatch(
            regionRule ->
                RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT.equals(
                    regionRule.getActionType()));
  }

  private boolean isDuplicateBlockAllExceptRuleUpdate(
      String id, Supplier<List<RegionRule>> existingRegionRulesSupplier) {
    return existingRegionRulesSupplier.get().stream()
        .anyMatch(
            regionRule ->
                RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT.equals(
                        regionRule.getActionType())
                    && !id.equals(regionRule.getId()));
  }
}
