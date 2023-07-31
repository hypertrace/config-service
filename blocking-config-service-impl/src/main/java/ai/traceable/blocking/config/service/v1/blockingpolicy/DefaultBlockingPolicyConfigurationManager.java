package ai.traceable.blocking.config.service.v1.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingPolicyConfiguration;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DefaultBlockingPolicyConfigurationManager implements BlockingPolicyConfigurationManager {
  private final GenericBlockingDetailsAggregator<BlockingDetails> blockingDetailsAggregator;
  private final UuidGenerator uuidGenerator;

  @Inject
  public DefaultBlockingPolicyConfigurationManager(
      GenericBlockingDetailsAggregator<BlockingDetails> blockingDetailsAggregator,
      UuidGenerator uuidGenerator) {
    this.blockingDetailsAggregator = blockingDetailsAggregator;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public BlockingPolicyConfiguration getBlockingPolicyConfiguration(
      RequestContext requestContext, String requestHash, Optional<String> environmentId) {
    try {
      List<BlockingDetails> blockingDetailsList =
          blockingDetailsAggregator.getBlockingDetails(
              requestContext,
              BlockingPolicyDataFilter.builder().environmentId(environmentId).build());
      String responseHash = uuidGenerator.generateId(blockingDetailsList);
      if (responseHash.equals(requestHash)) {
        return BlockingPolicyConfiguration.newBuilder().setHash(requestHash).build();
      }
      return BlockingPolicyConfiguration.newBuilder()
          .setHash(responseHash)
          .addAllBlockingDetailsList(blockingDetailsList)
          .build();
    } catch (Exception e) {
      log.error(
          "Unable to create blocking policy configuration for tenant:{} ",
          requestContext.getTenantId(),
          e);
    }
    return BlockingPolicyConfiguration.newBuilder().setHash(requestHash).build();
  }
}
