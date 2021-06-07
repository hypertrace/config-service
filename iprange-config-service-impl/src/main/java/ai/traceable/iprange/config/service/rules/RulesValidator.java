package ai.traceable.iprange.config.service.rules;

import ai.traceable.iprange.config.service.v1.CreateIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.DeleteIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.UpdateIpRangeRuleRequest;
import io.grpc.Status;

public interface RulesValidator {
  Status validate(CreateIpRangeRuleRequest request);

  Status validate(UpdateIpRangeRuleRequest request);

  Status validate(DeleteIpRangeRuleRequest request);
}
