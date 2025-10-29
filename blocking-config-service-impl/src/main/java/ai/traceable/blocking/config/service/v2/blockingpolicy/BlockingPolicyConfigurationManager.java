package ai.traceable.blocking.config.service.v2.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyAggregate;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.v2.AgentCapabilities;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.BlockingDetails;
import ai.traceable.blocking.config.service.v2.BlockingPolicyConfiguration;
import ai.traceable.blocking.config.service.v2.Component;
import ai.traceable.blocking.config.service.v2.ExclusionRule;
import ai.traceable.blocking.config.service.v2.IpResolutionStrategy;
import ai.traceable.blocking.config.service.v2.blockingpolicy.exclusion.ExclusionRuleConverter;
import ai.traceable.config.utils.SemanticVersioningComparator;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BlockingPolicyConfigurationManager implements BlockingConfigManagerBase {
  private final GenericBlockingDetailsAggregator<BlockingDetails> blockingDetailsAggregator;
  private final ExclusionRuleConverter exclusionRuleConverter;
  private final SemanticVersioningComparator semanticVersioningComparator;
  private final UuidGenerator uuidGenerator;

  @Inject
  public BlockingPolicyConfigurationManager(
      GenericBlockingDetailsAggregator<BlockingDetails> blockingDetailsAggregator,
      ExclusionRuleConverter exclusionRuleConverter,
      SemanticVersioningComparator semanticVersioningComparator,
      UuidGenerator uuidGenerator) {
    this.blockingDetailsAggregator = blockingDetailsAggregator;
    this.exclusionRuleConverter = exclusionRuleConverter;
    this.semanticVersioningComparator = semanticVersioningComparator;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public List<BlockingConfigResponseElement> generateBlockingElements(
      List<BlockingConfigRequestElement> requestElements,
      BlockingRulesSupplier blockingRulesSupplier) {

    requestElements =
        requestElements.stream()
            .filter(BlockingConfigRequestElement::hasBlockingPolicyConfigurationRequest)
            .collect(Collectors.toUnmodifiableList());

    if (requestElements.isEmpty()) {
      return List.of();
    }

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

    // Getting all the service-names
    List<String> serviceNames =
        requestElements.stream()
            .filter(BlockingConfigRequestElement::hasBlockingPolicyConfigurationRequest)
            .flatMap(requestElement -> requestElement.getSupportedAgentCapabilitiesList().stream())
            .map(this::getServiceName)
            .distinct()
            .collect(Collectors.toUnmodifiableList());

    BlockingPolicyDataFilter filter =
        BlockingPolicyDataFilter.builder()
            .environmentId(blockingRulesSupplier.getEnvironmentId())
            .serviceNames(serviceNames)
            .minLibtraceableVersion(minLibtraceableVersion)
            .build();

    BlockingPolicyAggregate<BlockingDetails> aggregate =
        blockingDetailsAggregator.getBlockingDetails(
            blockingRulesSupplier.getRequestContext(), filter, blockingRulesSupplier);

    Map<String, IpResolutionStrategy> serviceScopedIpResolutionStrategies =
        blockingRulesSupplier.getIpResolutionStrategies(new LinkedHashSet<>(serviceNames));

    Map<String, List<ExclusionRule>> serviceScopedExclusionRules =
        blockingRulesSupplier
            .getExclusionRules(new LinkedHashSet<>(serviceNames))
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        entry.getValue().stream()
                            .map(exclusionRuleConverter::convert)
                            .collect(Collectors.toList())));

    if (aggregate.getBlockingPolicyList() != null
        && serviceScopedExclusionRules.isEmpty()
        && serviceScopedIpResolutionStrategies.isEmpty()) {
      return Collections.singletonList(
          checkHashAndBuildResponse(
              requestElements.stream()
                  .map(BlockingConfigRequestElement::getPreviousHash)
                  .collect(Collectors.toUnmodifiableList()),
              new ServiceScopedInfo(aggregate.getBlockingPolicyList(), List.of(), null),
              requestElements.stream()
                  .map(BlockingConfigRequestElement::getSupportedAgentCapabilitiesList)
                  .flatMap(List::stream)
                  .collect(Collectors.toUnmodifiableList())));
    }

    Map<String, List<BlockingDetails>> serviceScopedBlockingPolicyMap;
    if (aggregate.getBlockingPolicyList() != null) {
      serviceScopedBlockingPolicyMap =
          serviceNames.stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      Function.identity(), a -> aggregate.getBlockingPolicyList()));
    } else {
      serviceScopedBlockingPolicyMap = aggregate.getServiceScopedBlockingPolicyMap();
    }

    Map<String, ServiceScopedInfo> serviceScopedInfoMap =
        mergeServiceScopedMaps(
            serviceScopedExclusionRules,
            serviceScopedBlockingPolicyMap,
            serviceScopedIpResolutionStrategies);

    return requestElements.stream()
        .map(
            requestElement ->
                buildServiceScopedResponseElements(requestElement, serviceScopedInfoMap))
        .flatMap(List::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private List<BlockingConfigResponseElement> buildServiceScopedResponseElements(
      BlockingConfigRequestElement requestElement,
      Map<String, ServiceScopedInfo> serviceScopedInfoMap) {
    return requestElement.getSupportedAgentCapabilitiesList().stream()
        .map(
            agentCapabilities ->
                new AbstractMap.SimpleEntry<>(
                    agentCapabilities,
                    Optional.ofNullable(
                        serviceScopedInfoMap.get(this.getServiceName(agentCapabilities)))))
        .collect(
            Collectors.groupingBy(
                Map.Entry::getValue,
                LinkedHashMap::new,
                Collectors.mapping(Map.Entry::getKey, Collectors.toUnmodifiableList())))
        .entrySet()
        .stream()
        .map(
            entry ->
                checkHashAndBuildResponse(
                    Collections.singletonList(requestElement.getPreviousHash()),
                    entry.getKey().orElse(new ServiceScopedInfo()),
                    entry.getValue()))
        .collect(Collectors.toUnmodifiableList());
  }

  private BlockingConfigResponseElement checkHashAndBuildResponse(
      List<String> previousHashes,
      ServiceScopedInfo serviceScopedInfo,
      List<AgentCapabilities> agentCapabilities) {
    BlockingPolicyConfiguration.Builder configBuilder =
        BlockingPolicyConfiguration.newBuilder()
            .addAllBlockingDetailsList(serviceScopedInfo.getBlockingDetails())
            .addAllExclusionRules(serviceScopedInfo.getExclusionRules());
    if (serviceScopedInfo.getIpResolutionStrategy() != null) {
      configBuilder.setIpResolutionStrategy(serviceScopedInfo.getIpResolutionStrategy());
    }
    BlockingPolicyConfiguration blockingPolicyConfiguration = configBuilder.build();
    String responseHash = uuidGenerator.generateId(blockingPolicyConfiguration);
    if (previousHashes.stream().allMatch(responseHash::equals)) {
      return BlockingConfigResponseElement.newBuilder()
          .setHash(responseHash)
          .setBlockingPolicyConfiguration(BlockingPolicyConfiguration.getDefaultInstance())
          .addAllAgentCapabilities(agentCapabilities)
          .build();
    }
    return BlockingConfigResponseElement.newBuilder()
        .setHash(responseHash)
        .setBlockingPolicyConfiguration(blockingPolicyConfiguration)
        .addAllAgentCapabilities(agentCapabilities)
        .build();
  }

  private static Map<String, ServiceScopedInfo> mergeServiceScopedMaps(
      Map<String, List<ExclusionRule>> serviceScopedExclusionRules,
      Map<String, List<BlockingDetails>> serviceScopedBlockingPolicyMap,
      Map<String, IpResolutionStrategy> serviceScopedIpResolutionStrategies) {
    return Stream.of(
            serviceScopedExclusionRules.keySet(),
            serviceScopedBlockingPolicyMap.keySet(),
            serviceScopedIpResolutionStrategies.keySet())
        .flatMap(java.util.Set::stream)
        .distinct()
        .collect(
            Collectors.toMap(
                serviceName -> serviceName,
                serviceName ->
                    new ServiceScopedInfo(
                        Optional.ofNullable(serviceScopedBlockingPolicyMap.get(serviceName))
                            .orElse(Collections.emptyList()),
                        Optional.ofNullable(serviceScopedExclusionRules.get(serviceName))
                            .orElse(Collections.emptyList()),
                        Optional.ofNullable(serviceScopedIpResolutionStrategies.get(serviceName))
                            .orElse(null))));
  }

  private String getServiceName(AgentCapabilities agentCapabilities) {
    return agentCapabilities.getComponentsList().stream()
        .filter(Component::hasServiceName)
        .map(Component::getServiceName)
        .findAny()
        .orElse("");
  }
}
