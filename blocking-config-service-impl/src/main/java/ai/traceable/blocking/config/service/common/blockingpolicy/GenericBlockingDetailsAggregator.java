package ai.traceable.blocking.config.service.common.blockingpolicy;

import com.google.inject.Inject;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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

  public List<T> getBlockingDetails(RequestContext requestContext, Optional<String> environmentId) {
    try {
      return blockingPolicyDataAggregator
          .getOrderedBlockingRules(requestContext, environmentId)
          .stream()
          .map(blockingDetailsConverter::convert)
          .flatMap(Collection::stream)
          .collect(Collectors.toUnmodifiableList());
    } catch (Exception e) {
      log.error(
          "Unable to create blocking policies for tenant:{} and environment:{} ",
          requestContext.getTenantId(),
          environmentId.orElse(""),
          e);
      return Collections.emptyList();
    }
  }

  public interface BlockingDetailsConverterBase<T> {
    List<T> convert(BlockingPolicyData blockingPolicyData);
  }
}
