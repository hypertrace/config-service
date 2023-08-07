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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CustomSignatureBlockingManager implements BlockingConfigManagerBase {
  private static final String MINIMUM_LIBTRACEABLE_VERSION_FOR_V3_SECARG = "0.1.98-rc.139";

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

    List<BlockingConfigResponseElement> responseElements = new ArrayList<>();
    buildResponseElement(
            customSignatureRequestElements,
            blockingRulesSupplier,
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3)
        .ifPresent(responseElements::add);
    buildResponseElement(
            customSignatureRequestElements,
            blockingRulesSupplier,
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS)
        .ifPresent(responseElements::add);
    return responseElements;
  }

  private Optional<BlockingConfigResponseElement> buildResponseElement(
      List<BlockingConfigRequestElement> requestElements,
      BlockingRulesSupplier blockingRulesSupplier,
      CustomModsecRuleVersion customModsecRuleVersion) {
    List<AgentCapabilities> matchingRequestAgentCapabilities = new ArrayList<>();
    List<String> previousHashes = new ArrayList<>();
    requestElements.forEach(
        requestElement -> {
          List<AgentCapabilities> filteredAgentCapabilities =
              requestElement.getSupportedAgentCapabilitiesList().stream()
                  .filter(
                      agentCapabilities ->
                          this.isModsecRuleVersionSupported(
                              agentCapabilities, customModsecRuleVersion))
                  .collect(Collectors.toUnmodifiableList());
          if (!filteredAgentCapabilities.isEmpty()) {
            previousHashes.add(requestElement.getPreviousHash());
            matchingRequestAgentCapabilities.addAll(filteredAgentCapabilities);
          }
        });

    // We don't want to send the rules to agents if they don't require it
    if (matchingRequestAgentCapabilities.isEmpty()) {
      return Optional.empty();
    }

    String customSignatureRulesBlob =
        blockingRulesSupplier.getCustomSignatureModsecBlob(customModsecRuleVersion);
    String responseHash = uuidGenerator.generateId(customSignatureRulesBlob);

    // Checking if hashes of all requests are same
    if (previousHashes.stream().allMatch(responseHash::equals)) {
      return Optional.of(
          BlockingConfigResponseElement.newBuilder()
              .setHash(responseHash)
              .addAllAgentCapabilities(matchingRequestAgentCapabilities)
              .setCustomSignatureBlockingRules(CustomSignatureBlockingRules.getDefaultInstance())
              .build());
    }

    return Optional.of(
        BlockingConfigResponseElement.newBuilder()
            .setHash(responseHash)
            .addAllAgentCapabilities(matchingRequestAgentCapabilities)
            .setCustomSignatureBlockingRules(
                CustomSignatureBlockingRules.newBuilder()
                    .setCustomSignatureRulesBlob(customSignatureRulesBlob))
            .build());
  }

  private boolean isModsecRuleVersionSupported(
      AgentCapabilities agentCapabilities, CustomModsecRuleVersion customModsecRuleVersion) {
    // An agent can be said to support seg arg limit if it has any component with desired
    // libtraceable version
    return (customModsecRuleVersion.equals(
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS))
        ? agentCapabilities.getComponentsList().stream().anyMatch(this::isSecArgLimitsSupported)
        : agentCapabilities.getComponentsList().stream().noneMatch(this::isSecArgLimitsSupported);
  }

  private boolean isSecArgLimitsSupported(Component component) {
    // A component can be said to support seg arg limit if it has desired libtraceable version
    return component.hasLibtraceableVersion()
        && semanticVersioningComparator.compare(
                component.getLibtraceableVersion(), MINIMUM_LIBTRACEABLE_VERSION_FOR_V3_SECARG)
            >= 0;
  }
}
