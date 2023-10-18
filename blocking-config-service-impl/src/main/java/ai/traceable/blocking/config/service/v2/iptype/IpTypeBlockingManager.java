package ai.traceable.blocking.config.service.v2.iptype;

import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.v2.AgentCapabilities;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.Component;
import ai.traceable.blocking.config.service.v2.IpTypeBlockingRules;
import ai.traceable.blocking.config.service.v2.IpTypeRule;
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
import org.apache.commons.lang3.tuple.ImmutablePair;

public class IpTypeBlockingManager implements BlockingConfigManagerBase {
  private final IpTypeRuleConverter ipTypeRuleConverter;
  private final UuidGenerator uuidGenerator;

  @Inject
  public IpTypeBlockingManager(
      IpTypeRuleConverter ipTypeRuleConverter, UuidGenerator uuidGenerator) {
    this.ipTypeRuleConverter = ipTypeRuleConverter;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public List<BlockingConfigResponseElement> generateBlockingElements(
      List<BlockingConfigRequestElement> requestElements,
      BlockingRulesSupplier blockingRulesSupplier) {
    List<BlockingConfigRequestElement> ipTypeRequestElements =
        requestElements.stream()
            .filter(BlockingConfigRequestElement::hasIpTypeBlockingRulesRequest)
            .collect(Collectors.toUnmodifiableList());
    if (ipTypeRequestElements.isEmpty()) {
      return Collections.emptyList();
    }

    List<AgentRequestComponent> serviceAgnosticComponents = new ArrayList<>();
    List<AgentRequestComponent> serviceBasedComponents = new ArrayList<>();
    Set<String> serviceNames = new LinkedHashSet<>();
    for (BlockingConfigRequestElement requestElement : ipTypeRequestElements) {
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

    Map<String, AgentResponseComponent<List<IpTypeRule>>> responseHashComponentsMap =
        new LinkedHashMap<>();
    if (!serviceAgnosticComponents.isEmpty()) {
      updateResponseHashComponentsMap(
          blockingRulesSupplier.getIpTypeIpMappings(
              ipTypeRuleConverter::convert, Collections.emptySet()),
          serviceAgnosticComponents,
          responseHashComponentsMap);
    }
    // for simplicity, we combine all service-based region info as single response..
    if (!serviceBasedComponents.isEmpty()) {
      updateResponseHashComponentsMap(
          blockingRulesSupplier.getIpTypeIpMappings(ipTypeRuleConverter::convert, serviceNames),
          serviceBasedComponents,
          responseHashComponentsMap);
    }

    return responseHashComponentsMap.values().stream()
        .map(this::buildResponseElement)
        .collect(Collectors.toUnmodifiableList());
  }

  private void updateResponseHashComponentsMap(
      List<ImmutablePair<IpTypeRule, String>> ipTypeBlockingRules,
      List<AgentRequestComponent> components,
      Map<String, AgentResponseComponent<List<IpTypeRule>>> responseHashComponentsMap) {
    // Calculating uuid of IpTypeBlob is very costly due to large size of the blob, hence using the
    // existing hash
    String responseHash =
        uuidGenerator.generateId(
            ipTypeBlockingRules.stream()
                .map(ImmutablePair::getRight)
                .collect(Collectors.toUnmodifiableList()));
    responseHashComponentsMap
        .computeIfAbsent(
            responseHash,
            hash ->
                new AgentResponseComponent<>(
                    ipTypeBlockingRules.stream()
                        .map(ImmutablePair::getLeft)
                        .collect(Collectors.toUnmodifiableList()),
                    responseHash))
        .getAgentRequestComponents()
        .addAll(components);
  }

  private BlockingConfigResponseElement buildResponseElement(
      AgentResponseComponent<List<IpTypeRule>> agentResponseComponent) {
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
      responseElementBuilder.setIpTypeBlockingRules(IpTypeBlockingRules.getDefaultInstance());
    } else {
      responseElementBuilder.setIpTypeBlockingRules(
          IpTypeBlockingRules.newBuilder()
              .addAllIpTypeRuleList(agentResponseComponent.getResponseData()));
    }

    return responseElementBuilder.build();
  }
}
