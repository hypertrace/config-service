package ai.traceable.blocking.config.service.v2.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.BlockingDetails;
import ai.traceable.blocking.config.service.v2.BlockingPolicyConfiguration;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class BlockingPolicyConfigurationManager implements BlockingConfigManagerBase {
  private final GenericBlockingDetailsAggregator<BlockingDetails> blockingDetailsAggregator;
  private final UuidGenerator uuidGenerator;

  @Inject
  public BlockingPolicyConfigurationManager(
      GenericBlockingDetailsAggregator<BlockingDetails> blockingDetailsAggregator,
      UuidGenerator uuidGenerator) {
    this.blockingDetailsAggregator = blockingDetailsAggregator;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public List<BlockingConfigResponseElement> generateBlockingElements(
      List<BlockingConfigRequestElement> requestElements,
      RequestContext requestContext,
      Optional<String> environmentId) {
    List<BlockingConfigRequestElement> blockingPolicyRequestElements =
        requestElements.stream()
            .filter(BlockingConfigRequestElement::hasBlockingPolicyConfigurationRequest)
            .collect(Collectors.toUnmodifiableList());
    if (blockingPolicyRequestElements.isEmpty()) {
      return List.of();
    }

    BlockingPolicyConfiguration blockingPolicyConfiguration =
        BlockingPolicyConfiguration.newBuilder()
            .addAllBlockingDetailsList(
                blockingDetailsAggregator.getBlockingDetails(requestContext, environmentId))
            .build();
    return List.of(
        buildResponseElement(blockingPolicyRequestElements, blockingPolicyConfiguration));
  }

  private BlockingConfigResponseElement buildResponseElement(
      List<BlockingConfigRequestElement> requestElements,
      BlockingPolicyConfiguration blockingPolicyConfiguration) {
    String responseHash = uuidGenerator.generateId(blockingPolicyConfiguration);
    if (requestElements.stream()
        .map(BlockingConfigRequestElement::getPreviousHash)
        .allMatch(responseHash::equals)) {
      return BlockingConfigResponseElement.newBuilder()
          .setHash(responseHash)
          .setBlockingPolicyConfiguration(BlockingPolicyConfiguration.getDefaultInstance())
          .build();
    }
    return BlockingConfigResponseElement.newBuilder()
        .setHash(responseHash)
        .setBlockingPolicyConfiguration(blockingPolicyConfiguration)
        .build();
  }
}
