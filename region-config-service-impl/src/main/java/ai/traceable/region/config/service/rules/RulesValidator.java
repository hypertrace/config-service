package ai.traceable.region.config.service.rules;

import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import io.grpc.Status;

public interface RulesValidator {
  Status validate(CreateRegionRuleRequest request);
}
