package ai.traceable.malicioussources.config.service.rules;

import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.DeleteMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.UpdateMaliciousSourcesRuleRequest;
import io.grpc.Status;
import java.util.List;

public interface RulesValidator {
  Status validate(CreateMaliciousSourcesRuleRequest request, List<MaliciousSourcesRule> rules);

  Status validate(UpdateMaliciousSourcesRuleRequest request, List<MaliciousSourcesRule> rules);

  Status validate(DeleteMaliciousSourcesRuleRequest request);
}
