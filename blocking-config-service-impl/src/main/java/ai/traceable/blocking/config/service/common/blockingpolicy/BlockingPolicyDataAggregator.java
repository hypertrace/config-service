package ai.traceable.blocking.config.service.common.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import com.google.inject.Inject;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

class BlockingPolicyDataAggregator {
  private final Set<BlockingPolicyDataFetcherBase> blockingPolicyDataFetchers;

  @Inject
  BlockingPolicyDataAggregator(Set<BlockingPolicyDataFetcherBase> blockingPolicyDataFetchers) {
    this.blockingPolicyDataFetchers = blockingPolicyDataFetchers;
  }

  List<BlockingPolicyData> getOrderedBlockingRules(
      RequestContext requestContext, BlockingPolicyDataFilter filter) {
    // BlockingPolicyData is sorted according to BlockingPolicyDataBucket Enum.
    return blockingPolicyDataFetchers.stream()
        .flatMap(
            blockingConfigManagerBase ->
                blockingConfigManagerBase.getBlockingPolicyData(requestContext, filter).stream())
        .sorted(Comparator.comparing(BlockingPolicyData::getBucket))
        .collect(Collectors.toUnmodifiableList());
  }
}
