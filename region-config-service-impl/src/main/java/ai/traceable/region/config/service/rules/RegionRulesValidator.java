package ai.traceable.region.config.service.rules;

import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import io.grpc.Status;

class RegionRulesValidator implements RulesValidator {

  @Override
  public Status validate(CreateRegionRuleRequest request) {
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

    return Status.OK;
  }
}
