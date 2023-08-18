package ai.traceable.blocking.config.service.v2.regions;

import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.v2.AgentCapabilities;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.Component;
import ai.traceable.blocking.config.service.v2.RegionBlockingRules;
import ai.traceable.blocking.config.service.v2.RegionIpBlockingRule;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RegionBlockingManager implements BlockingConfigManagerBase {
  private final RegionIpRulesConverter regionIpRulesConverter;
  private final UuidGenerator uuidGenerator;

  @Inject
  public RegionBlockingManager(
      RegionIpRulesConverter regionIpRulesConverter, UuidGenerator uuidGenerator) {
    this.regionIpRulesConverter = regionIpRulesConverter;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public List<BlockingConfigResponseElement> generateBlockingElements(
      List<BlockingConfigRequestElement> requestElements,
      BlockingRulesSupplier blockingRulesSupplier) {
    List<BlockingConfigRequestElement> regionRequestElements =
        requestElements.stream()
            .filter(BlockingConfigRequestElement::hasRegionBlockingRulesRequest)
            .collect(Collectors.toUnmodifiableList());
    if (regionRequestElements.isEmpty()) {
      return List.of();
    }

    List<AgentRequestComponent> serviceAgnosticComponents = new ArrayList<>();
    List<AgentRequestComponent> serviceBasedComponents = new ArrayList<>();
    Set<String> serviceNames = new LinkedHashSet<>();
    for (BlockingConfigRequestElement requestElement : regionRequestElements) {
      for (AgentCapabilities agentCapabilities :
          requestElement.getSupportedAgentCapabilitiesList()) {
        AgentRequestComponent agentRequestComponent =
            new AgentRequestComponent(agentCapabilities, requestElement.getPreviousHash());
        List<String> services =
            agentCapabilities.getComponentsList().stream()
                .filter(Component::hasServiceName)
                .map(Component::getServiceName)
                .collect(Collectors.toList());
        if (services.isEmpty()) {
          serviceAgnosticComponents.add(agentRequestComponent);
        } else {
          serviceBasedComponents.add(agentRequestComponent);
          serviceNames.addAll(services);
        }
      }
    }

    Map<String, AgentResponseComponent<List<RegionIpBlockingRule>>> responseHashComponentsMap =
        new LinkedHashMap<>();
    if (!serviceAgnosticComponents.isEmpty()) {
      // dlp rules are not supported..
      updateResponseHashComponentsMap(
          blockingRulesSupplier.getRegionIpMappings(
              regionIpRulesConverter::convert, Collections.emptySet()),
          serviceAgnosticComponents,
          responseHashComponentsMap);
    }
    // for simplicity, we combine all service-based region info as single response..
    if (!serviceBasedComponents.isEmpty()) {
      updateResponseHashComponentsMap(
          blockingRulesSupplier.getRegionIpMappings(regionIpRulesConverter::convert, serviceNames),
          serviceBasedComponents,
          responseHashComponentsMap);
    }

    return responseHashComponentsMap.values().stream()
        .map(this::buildResponseElement)
        .collect(Collectors.toUnmodifiableList());
  }

  private void updateResponseHashComponentsMap(
      List<RegionIpBlockingRule> regionBlockingRules,
      List<AgentRequestComponent> components,
      Map<String, AgentResponseComponent<List<RegionIpBlockingRule>>> responseHashComponentsMap) {
    String responseHash = uuidGenerator.generateId(regionBlockingRules);
    responseHashComponentsMap
        .computeIfAbsent(
            responseHash, hash -> new AgentResponseComponent<>(regionBlockingRules, responseHash))
        .getAgentRequestComponents()
        .addAll(components);
  }

  private BlockingConfigResponseElement buildResponseElement(
      AgentResponseComponent<List<RegionIpBlockingRule>> agentResponseComponent) {
    String responseHash = agentResponseComponent.getResponseHash();
    List<AgentRequestComponent> agentRequestComponentsList =
        agentResponseComponent.getAgentRequestComponents();

    BlockingConfigResponseElement.Builder responseElementBuilder =
        BlockingConfigResponseElement.newBuilder()
            .setHash(responseHash)
            .addAllAgentCapabilities(
                agentRequestComponentsList.stream()
                    .map(AgentRequestComponent::getMatchingRequestAgentCapabilities)
                    .collect(Collectors.toList()));

    if (agentRequestComponentsList.stream()
        .allMatch(
            agentRequestComponent ->
                responseHash.equals(agentRequestComponent.getPreviousHash()))) {
      responseElementBuilder.setRegionBlockingRules(RegionBlockingRules.getDefaultInstance());
    } else {
      responseElementBuilder.setRegionBlockingRules(
          RegionBlockingRules.newBuilder()
              .addAllRegionIpBlockingRules(agentResponseComponent.getResponseData()));
    }

    return responseElementBuilder.build();
  }
}
