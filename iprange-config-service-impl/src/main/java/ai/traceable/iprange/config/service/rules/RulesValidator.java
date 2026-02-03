package ai.traceable.iprange.config.service.rules;

import ai.traceable.iprange.config.service.v1.BulkDeleteIpRangeRulesRequest;
import ai.traceable.iprange.config.service.v1.BulkUpdateIpRangeRulesRequest;
import ai.traceable.iprange.config.service.v1.CreateIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.DeleteIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import ai.traceable.iprange.config.service.v1.UpdateIpRangeRuleRequest;
import io.grpc.Status;
import java.util.List;
import java.util.function.Supplier;

public interface RulesValidator {
  Status validate(
      CreateIpRangeRuleRequest request, Supplier<List<IpRangeRule>> ipRangeRuleSupplier);

  Status validate(
      UpdateIpRangeRuleRequest request, Supplier<List<IpRangeRule>> ipRangeRuleSupplier);

  Status validate(DeleteIpRangeRuleRequest request);

  Status validate(BulkDeleteIpRangeRulesRequest request);

  Status validate(BulkUpdateIpRangeRulesRequest request);
}
