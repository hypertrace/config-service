package ai.traceable.blocking.config.service.v2.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator;
import ai.traceable.blocking.config.service.v2.BlockingDetails;
import ai.traceable.blocking.config.service.v2.BlockingPolicyConfiguration;
import com.google.inject.Inject;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DefaultBlockingPolicyConfigurationManager
    implements BlockingPolicyConfigurationManager {
  private final GenericBlockingDetailsAggregator<BlockingDetails> blockingDetailsAggregator;

  @Inject
  public DefaultBlockingPolicyConfigurationManager(
      GenericBlockingDetailsAggregator<BlockingDetails> blockingDetailsAggregator) {
    this.blockingDetailsAggregator = blockingDetailsAggregator;
  }

  @Override
  public BlockingPolicyConfiguration getBlockingPolicyConfiguration(
      RequestContext requestContext, Optional<String> environmentId) {
    return BlockingPolicyConfiguration.newBuilder()
        .addAllBlockingDetailsList(
            blockingDetailsAggregator.getBlockingDetails(requestContext, environmentId))
        .build();
  }
}
