package ai.traceable.blocking.config.service.common.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.DataFetcherBase;
import com.google.inject.Inject;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

class BlockingPolicyDataAggregator {
  private final Set<DataFetcherBase> dataFetcherBases;

  @Inject
  BlockingPolicyDataAggregator(Set<DataFetcherBase> dataFetcherBases) {
    this.dataFetcherBases = dataFetcherBases;
  }

  List<BlockingPolicyData> getOrderedBlockingRules(
      RequestContext requestContext, Optional<String> environmentId) {
    // BlockingPolicyData is sorted according to BlockingPolicyDataBucket Enum.
    return dataFetcherBases.stream()
        .flatMap(
            blockingConfigManagerBase ->
                blockingConfigManagerBase
                    .getBlockingPolicyData(requestContext, environmentId)
                    .stream())
        .sorted(Comparator.comparing(a -> a.bucket))
        .collect(Collectors.toUnmodifiableList());
  }
}
