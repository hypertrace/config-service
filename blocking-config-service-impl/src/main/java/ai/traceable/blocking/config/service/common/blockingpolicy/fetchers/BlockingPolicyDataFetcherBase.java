package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Singular;
import lombok.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface BlockingPolicyDataFetcherBase {
  BlockingPolicyAggregate<BlockingPolicyData> getBlockingPolicyData(
      RequestContext requestContext,
      BlockingPolicyDataFilter filter,
      BlockingRulesSupplier blockingRulesSupplier);

  @Value
  @Builder
  class BlockingPolicyDataFilter {
    @Singular List<String> serviceNames;
    Optional<String> environmentId;
    @Default String minLibtraceableVersion = "";
  }
}
