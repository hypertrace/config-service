package ai.traceable.blocking.config.service.common.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyAggregate;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;

class BlockingPolicyDataAggregator {
  private final Set<BlockingPolicyDataFetcherBase> blockingPolicyDataFetchers;

  @Inject
  BlockingPolicyDataAggregator(Set<BlockingPolicyDataFetcherBase> blockingPolicyDataFetchers) {
    this.blockingPolicyDataFetchers = blockingPolicyDataFetchers;
  }

  BlockingPolicyAggregate<BlockingPolicyData> getOrderedBlockingRules(
      RequestContext requestContext,
      BlockingPolicyDataFilter filter,
      BlockingRulesSupplier blockingRulesSupplier) {
    // BlockingPolicyData is sorted according to BlockingPolicyDataBucket Enum.
    return blockingPolicyDataFetchers.stream()
        .map(
            blockingConfigManagerBase ->
                blockingConfigManagerBase.getBlockingPolicyData(
                    requestContext, filter, blockingRulesSupplier))
        .reduce(BlockingPolicyAggregate::merge)
        .map(
            blockingPolicyAggregate ->
                BlockingPolicyAggregate.sort(
                    blockingPolicyAggregate, Comparator.comparing(BlockingPolicyData::getBucket)))
        .orElse(new BlockingPolicyAggregate<>(Collections.emptyList()));
  }
}
