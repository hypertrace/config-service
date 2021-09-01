package ai.traceable.region.config.service.rules;

import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.DeleteRegionRuleRequest;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.UpdateRegionRuleRequest;
import io.grpc.Status;
import java.util.List;
import java.util.function.Supplier;

public interface RulesValidator {
  Status validate(
      CreateRegionRuleRequest request, Supplier<List<RegionRule>> existingRegionRulesSupplier);

  Status validate(
      UpdateRegionRuleRequest request, Supplier<List<RegionRule>> existingRegionRulesSupplier);

  Status validate(DeleteRegionRuleRequest request);
}
