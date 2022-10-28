package ai.traceable.malicioussources.config.service.rules;

import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.DeleteMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.UpdateMaliciousSourcesRuleRequest;
import io.grpc.Status;
import java.util.List;
import java.util.function.Supplier;

public interface RulesValidator {
  Status validate(
      CreateMaliciousSourcesRuleRequest request,
      Supplier<List<MaliciousSourcesRule>> blockAllExceptRulesSupplier);

  Status validate(
      UpdateMaliciousSourcesRuleRequest request,
      Supplier<List<MaliciousSourcesRule>> blockAllExceptRulesSupplier);

  Status validate(DeleteMaliciousSourcesRuleRequest request);
}
