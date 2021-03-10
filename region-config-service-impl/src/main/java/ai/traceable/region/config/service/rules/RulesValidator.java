package ai.traceable.region.config.service.rules;

import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.DeleteRegionRuleRequest;
import ai.traceable.region.config.service.v1.UpdateRegionRuleRequest;
import io.grpc.Status;

public interface RulesValidator {
  Status validate(CreateRegionRuleRequest request);

  Status validate(UpdateRegionRuleRequest request);

  Status validate(DeleteRegionRuleRequest request);
}
