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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
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

    Map<CustomModsecRuleVersion, AgentRequestComponents> requestElementsMap = new LinkedHashMap<>();

    for (BlockingConfigRequestElement requestElement : customSignatureRequestElements) {
      for (AgentCapabilities agentCapabilities :
          requestElement.getSupportedAgentCapabilitiesList()) {
        CustomModsecRuleVersion modsecRuleVersion =
            getMaximumSupportedModsecRuleVersion(agentCapabilities);
        AgentRequestComponents agentRequestComponents =
            requestElementsMap.computeIfAbsent(
                modsecRuleVersion, v -> new AgentRequestComponents());
        agentRequestComponents.getMatchingRequestAgentCapabilities().add(agentCapabilities);
        agentRequestComponents.getPreviousHashes().add(requestElement.getPreviousHash());
      }
    }

    return requestElementsMap.entrySet().stream()
        .map(entry -> buildResponseElement(entry.getKey(), entry.getValue(), blockingRulesSupplier))
        .collect(Collectors.toUnmodifiableList());
  }

  private BlockingConfigResponseElement buildResponseElement(
      CustomModsecRuleVersion customModsecRuleVersion,
      AgentRequestComponents agentRequestComponents,
      BlockingRulesSupplier blockingRulesSupplier) {

    String customSignatureRulesBlob =
        blockingRulesSupplier.getCustomSignatureModsecBlob(customModsecRuleVersion);
    String responseHash = uuidGenerator.generateId(customSignatureRulesBlob);

    // Checking if hashes of all requests are same
    if (agentRequestComponents.getPreviousHashes().stream().allMatch(responseHash::equals)) {
      return BlockingConfigResponseElement.newBuilder()
          .setHash(responseHash)
          .addAllAgentCapabilities(agentRequestComponents.getMatchingRequestAgentCapabilities())
          .setCustomSignatureBlockingRules(CustomSignatureBlockingRules.getDefaultInstance())
          .build();
    }

    return BlockingConfigResponseElement.newBuilder()
        .setHash(responseHash)
        .addAllAgentCapabilities(agentRequestComponents.getMatchingRequestAgentCapabilities())
        .setCustomSignatureBlockingRules(
            CustomSignatureBlockingRules.newBuilder()
                .setCustomSignatureRulesBlob(customSignatureRulesBlob))
        .build();
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

  @Getter(AccessLevel.PACKAGE)
  static class AgentRequestComponents {
    private final List<AgentCapabilities> matchingRequestAgentCapabilities = new ArrayList<>();
    private final Set<String> previousHashes = new HashSet<>();
  }
}
