package ai.traceable.blocking.config.service.v2.customsignature;

import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.v2.AgentCapabilities;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.Component;
import ai.traceable.blocking.config.service.v2.CustomSignatureBlockingRules;
import ai.traceable.config.utils.SemanticVersioningComparator;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomSignatureBlockingManager implements BlockingConfigManagerBase {
  private static final String MINIMUM_LIBTRACEABLE_VERSION_FOR_V3_SECARG = "0.1.98-rc.139";
  private static final String MINIMUM_LIBTRACEABLE_VERSION_FOR_V3_SECARG_DETECTION_ONLY =
      "0.1.98-rc.162";
  private static final List<CustomModsecRuleVersion> MODSEC_VERSIONS_IN_DECREASING_PRIORITY =
      List.of(
          CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE,
          CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS,
          CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3);
  private static final CustomModsecRuleVersion DEFAULT_LOWEST_MODSEC_VERSION =
      CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3;

  private final UuidGenerator uuidGenerator;
  private final SemanticVersioningComparator semanticVersioningComparator;

  @Inject
  public CustomSignatureBlockingManager(
      UuidGenerator uuidGenerator, SemanticVersioningComparator semanticVersioningComparator) {
    this.uuidGenerator = uuidGenerator;
    this.semanticVersioningComparator = semanticVersioningComparator;
  }

  @Override
  public List<BlockingConfigResponseElement> generateBlockingElements(
      List<BlockingConfigRequestElement> requestElements,
      BlockingRulesSupplier blockingRulesSupplier) {

    List<BlockingConfigRequestElement> customSignatureRequestElements =
        requestElements.stream()
            .filter(BlockingConfigRequestElement::hasCustomSignatureBlockingRulesRequest)
            .collect(Collectors.toUnmodifiableList());
    if (customSignatureRequestElements.isEmpty()) {
      return Collections.emptyList();
    }

    Map<CustomModsecRuleVersion, List<AgentRequestComponent>> agentComponentsMap =
        new LinkedHashMap<>();

    for (BlockingConfigRequestElement requestElement : customSignatureRequestElements) {
      for (AgentCapabilities agentCapabilities :
          requestElement.getSupportedAgentCapabilitiesList()) {
        CustomModsecRuleVersion modsecRuleVersion =
            getMaximumSupportedModsecRuleVersion(agentCapabilities);
        AgentRequestComponent agentRequestComponent =
            new AgentRequestComponent(agentCapabilities, requestElement.getPreviousHash());
        agentComponentsMap
            .computeIfAbsent(modsecRuleVersion, v -> new ArrayList<>())
            .add(agentRequestComponent);
      }
    }

    return agentComponentsMap.entrySet().stream()
        .flatMap(
            entry ->
                buildAgentResponseComponents(
                        entry.getKey(), entry.getValue(), blockingRulesSupplier)
                    .stream()
                    .map(this::buildResponseElement))
        .collect(Collectors.toUnmodifiableList());
  }

  private Collection<AgentResponseComponent> buildAgentResponseComponents(
      CustomModsecRuleVersion customModsecRuleVersion,
      List<AgentRequestComponent> agentRequestComponents,
      BlockingRulesSupplier blockingRulesSupplier) {

    List<AgentRequestComponent> serviceAgnosticComponents = new ArrayList<>();
    Map<String, List<AgentRequestComponent>> serviceComponentsMap = new LinkedHashMap<>();
    for (AgentRequestComponent agentRequestComponent : agentRequestComponents) {
      Optional<String> serviceName =
          agentRequestComponent.getMatchingRequestAgentCapabilities().getComponentsList().stream()
              .filter(Component::hasServiceName)
              .findAny()
              .map(Component::getServiceName);
      if (serviceName.isPresent()) {
        serviceComponentsMap
            .computeIfAbsent(serviceName.get(), sName -> new ArrayList<>())
            .add(agentRequestComponent);
      } else {
        serviceAgnosticComponents.add(agentRequestComponent);
      }
    }

    Map<String, AgentResponseComponent> responseHashComponentsMap = new HashMap<>();
    if (!serviceAgnosticComponents.isEmpty()) {
      updateResponseHashComponentsMap(
          blockingRulesSupplier.getCustomSignatureModsecBlob(customModsecRuleVersion),
          serviceAgnosticComponents,
          responseHashComponentsMap);
    }
    if (!serviceComponentsMap.isEmpty()) {
      blockingRulesSupplier
          .getCustomSignatureModsecBlobs(
              customModsecRuleVersion, new ArrayList<>(serviceComponentsMap.keySet()))
          .forEach(
              (serviceName, customSignatureRulesBlob) ->
                  updateResponseHashComponentsMap(
                      customSignatureRulesBlob,
                      serviceComponentsMap.get(serviceName),
                      responseHashComponentsMap));
    }

    return responseHashComponentsMap.values();
  }

  private void updateResponseHashComponentsMap(
      String customSignatureRulesBlob,
      List<AgentRequestComponent> components,
      Map<String, AgentResponseComponent> responseHashComponentsMap) {
    String responseHash = uuidGenerator.generateId(customSignatureRulesBlob);
    responseHashComponentsMap
        .computeIfAbsent(
            responseHash,
            hash -> new AgentResponseComponent(responseHash, customSignatureRulesBlob))
        .getAgentRequestComponents()
        .addAll(components);
  }

  private BlockingConfigResponseElement buildResponseElement(
      AgentResponseComponent agentResponseComponent) {
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
      responseElementBuilder.setCustomSignatureBlockingRules(
          CustomSignatureBlockingRules.getDefaultInstance());
    } else {
      responseElementBuilder.setCustomSignatureBlockingRules(
          CustomSignatureBlockingRules.newBuilder()
              .setCustomSignatureRulesBlob(agentResponseComponent.getCustomSignatureModsecBlob()));
    }

    return responseElementBuilder.build();
  }

  private CustomModsecRuleVersion getMaximumSupportedModsecRuleVersion(
      AgentCapabilities agentCapabilities) {
    return agentCapabilities.getComponentsList().stream()
        .map(this::getMaximumSupportedModsecRuleVersion)
        .min(Comparator.comparing(MODSEC_VERSIONS_IN_DECREASING_PRIORITY::indexOf))
        .orElse(DEFAULT_LOWEST_MODSEC_VERSION);
  }

  private CustomModsecRuleVersion getMaximumSupportedModsecRuleVersion(Component component) {
    if (component.hasLibtraceableVersion()) {
      String libTraceableVersion = component.getLibtraceableVersion();
      try {
        if (semanticVersioningComparator.compare(
                libTraceableVersion, MINIMUM_LIBTRACEABLE_VERSION_FOR_V3_SECARG_DETECTION_ONLY)
            >= 0) {
          return CustomModsecRuleVersion
              .CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE;
        }
        if (semanticVersioningComparator.compare(
                libTraceableVersion, MINIMUM_LIBTRACEABLE_VERSION_FOR_V3_SECARG)
            >= 0) {
          return CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS;
        }
      } catch (Exception e) {
        log.error(
            "Error in comparing the semantic version for libtraceable:{}", libTraceableVersion, e);
      }
    }
    return CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3;
  }

  @Value
  private static class AgentRequestComponent {
    private final AgentCapabilities matchingRequestAgentCapabilities;
    private final String previousHash;
  }

  @Value
  private static class AgentResponseComponent {
    private final String responseHash;
    private final String customSignatureModsecBlob;
    private final List<AgentRequestComponent> agentRequestComponents = new ArrayList<>();
  }
}
