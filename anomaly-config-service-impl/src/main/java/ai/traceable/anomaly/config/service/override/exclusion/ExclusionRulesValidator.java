package ai.traceable.anomaly.config.service.override.exclusion;

import ai.traceable.anomaly.config.service.v1.override.CreateDetectionExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.override.DeleteDetectionExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.override.UpdateDetectionExclusionRuleRequest;
import io.grpc.Status;

public interface ExclusionRulesValidator {

  Status validate(CreateDetectionExclusionRuleRequest request);

  Status validate(UpdateDetectionExclusionRuleRequest request);

  Status validate(DeleteDetectionExclusionRuleRequest request);
}
