package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface BlockingPolicyDataFetcherBase {
  List<BlockingPolicyData> getBlockingPolicyData(
      RequestContext requestContext, BlockingPolicyDataFilter filter);

  @Value
  @Builder
  class BlockingPolicyDataFilter {
    List<String> serviceNames;
    Optional<String> environmentId;
  }
}
