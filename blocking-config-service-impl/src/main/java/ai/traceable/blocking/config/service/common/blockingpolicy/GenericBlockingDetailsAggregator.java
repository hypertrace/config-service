package ai.traceable.blocking.config.service.common.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
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

  public List<T> getBlockingDetails(
      RequestContext requestContext, BlockingPolicyDataFilter filter) {
    try {
      return blockingPolicyDataAggregator.getOrderedBlockingRules(requestContext, filter).stream()
          .map(blockingDetailsConverter::convert)
          .filter(Objects::nonNull)
          .collect(Collectors.toUnmodifiableList());
    } catch (Exception e) {
      log.error(
          "Unable to create blocking policies for tenant:{} with filter:{} ",
          requestContext.getTenantId(),
          filter,
          e);
      return Collections.emptyList();
    }
  }

  public interface BlockingDetailsConverterBase<T> {
    T convert(BlockingPolicyData blockingPolicyData);
  }
}
