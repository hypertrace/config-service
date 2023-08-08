package ai.traceable.blocking.config.service.v2.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.BlockingDetails;
import ai.traceable.blocking.config.service.v2.BlockingPolicyConfiguration;
import ai.traceable.blocking.config.service.v2.Component;
import ai.traceable.config.utils.SemanticVersioningComparator;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

public class BlockingPolicyConfigurationManager implements BlockingConfigManagerBase {
  private final GenericBlockingDetailsAggregator<BlockingDetails> blockingDetailsAggregator;
  private final SemanticVersioningComparator semanticVersioningComparator;
  private final UuidGenerator uuidGenerator;

  @Inject
  public BlockingPolicyConfigurationManager(
      GenericBlockingDetailsAggregator<BlockingDetails> blockingDetailsAggregator,
      SemanticVersioningComparator semanticVersioningComparator,
      UuidGenerator uuidGenerator) {
    this.blockingDetailsAggregator = blockingDetailsAggregator;
    this.semanticVersioningComparator = semanticVersioningComparator;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public List<BlockingConfigResponseElement> generateBlockingElements(
      List<BlockingConfigRequestElement> requestElements,
      BlockingRulesSupplier blockingRulesSupplier) {
    List<BlockingConfigRequestElement> blockingPolicyRequestElements =
        requestElements.stream()
            .filter(BlockingConfigRequestElement::hasBlockingPolicyConfigurationRequest)
            .collect(Collectors.toUnmodifiableList());

    // Getting minimum the libtraceable version mentioned
    String minLibtraceableVersion =
        requestElements.stream()
            .filter(BlockingConfigRequestElement::hasBlockingPolicyConfigurationRequest)
            .flatMap(requestElement -> requestElement.getSupportedAgentCapabilitiesList().stream())
            .flatMap(agentCapabilities -> agentCapabilities.getComponentsList().stream())
            .filter(Component::hasLibtraceableVersion)
            .map(Component::getLibtraceableVersion)
            .min(semanticVersioningComparator)
            .orElse("");

    // Getting all the service-names explicitly mentioned
    List<String> serviceNames =
        requestElements.stream()
            .filter(BlockingConfigRequestElement::hasBlockingPolicyConfigurationRequest)
            .flatMap(requestElement -> requestElement.getSupportedAgentCapabilitiesList().stream())
            .flatMap(agentCapabilities -> agentCapabilities.getComponentsList().stream())
            .filter(Component::hasServiceName)
            .map(Component::getServiceName)
            .distinct()
            .collect(Collectors.toUnmodifiableList());

    if (blockingPolicyRequestElements.isEmpty()) {
      return List.of();
    }

    BlockingPolicyDataFilter filter =
        BlockingPolicyDataFilter.builder()
            .environmentId(blockingRulesSupplier.getEnvironmentId())
            .serviceNames(serviceNames)
            .minLibtraceableVersion(minLibtraceableVersion)
            .build();
    BlockingPolicyConfiguration blockingPolicyConfiguration =
        BlockingPolicyConfiguration.newBuilder()
            .addAllBlockingDetailsList(
                blockingDetailsAggregator.getBlockingDetails(
                    blockingRulesSupplier.getRequestContext(), filter))
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
          .addAllAgentCapabilities(
              requestElements.stream()
                  .map(BlockingConfigRequestElement::getSupportedAgentCapabilitiesList)
                  .flatMap(List::stream)
                  .collect(Collectors.toUnmodifiableList()))
          .build();
    }
    return BlockingConfigResponseElement.newBuilder()
        .setHash(responseHash)
        .setBlockingPolicyConfiguration(blockingPolicyConfiguration)
        .addAllAgentCapabilities(
            requestElements.stream()
                .map(BlockingConfigRequestElement::getSupportedAgentCapabilitiesList)
                .flatMap(List::stream)
                .collect(Collectors.toUnmodifiableList()))
        .build();
  }
}
