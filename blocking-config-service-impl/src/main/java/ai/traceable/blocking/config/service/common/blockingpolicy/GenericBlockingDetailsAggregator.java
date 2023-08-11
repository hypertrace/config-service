package ai.traceable.blocking.config.service.common.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyAggregate;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import com.google.inject.Inject;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class GenericBlockingDetailsAggregator<T> {
  private final BlockingPolicyDataAggregator blockingPolicyDataAggregator;
  private final BlockingDetailsConverterBase<T> blockingDetailsConverter;

  @Inject
  GenericBlockingDetailsAggregator(
      BlockingPolicyDataAggregator blockingPolicyDataAggregator,
      BlockingDetailsConverterBase<T> blockingDetailsConverter) {
    this.blockingPolicyDataAggregator = blockingPolicyDataAggregator;
    this.blockingDetailsConverter = blockingDetailsConverter;
  }

  public BlockingPolicyAggregate<T> getBlockingDetails(
      RequestContext requestContext,
      BlockingPolicyDataFilter filter,
      BlockingRulesSupplier blockingRulesSupplier) {
    try {
      return BlockingPolicyAggregate.convert(
          blockingPolicyDataAggregator.getOrderedBlockingRules(
              requestContext, filter, blockingRulesSupplier),
          blockingPolicyData -> blockingDetailsConverter.convert(blockingPolicyData, filter));
    } catch (Exception e) {
      log.error(
          "Unable to create blocking policies for tenant:{} with filter:{} ",
          requestContext.getTenantId(),
          filter,
          e);
      return new BlockingPolicyAggregate<>(Collections.emptyList());
    }
  }

  public interface BlockingDetailsConverterBase<T> {
    T convert(BlockingPolicyData blockingPolicyData, BlockingPolicyDataFilter filter);
  }
}
