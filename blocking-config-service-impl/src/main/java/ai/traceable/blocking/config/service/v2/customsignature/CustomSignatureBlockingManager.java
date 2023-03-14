package ai.traceable.blocking.config.service.v2.customsignature;

import ai.traceable.blocking.config.service.common.customsignature.CustomSignatureBlobFetcher;
import ai.traceable.blocking.config.service.common.util.SemanticVersioningComparator;
import ai.traceable.blocking.config.service.v2.AgentCapabilities;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.Component;
import ai.traceable.blocking.config.service.v2.CustomSignatureBlockingRules;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class CustomSignatureBlockingManager implements BlockingConfigManagerBase {
  private static final String MINIMUM_LIBTRACEABLE_VERSION_FOR_V3_SECARG = "0.1.98-rc.139";

  private final CustomSignatureBlobFetcher customSignatureBlobFetcher;
  private final UuidGenerator uuidGenerator;
  private final SemanticVersioningComparator semanticVersioningComparator;

  @Inject
  public CustomSignatureBlockingManager(
      CustomSignatureBlobFetcher customSignatureBlobFetcher,
      UuidGenerator uuidGenerator,
      SemanticVersioningComparator semanticVersioningComparator) {
    this.customSignatureBlobFetcher = customSignatureBlobFetcher;
    this.uuidGenerator = uuidGenerator;
    this.semanticVersioningComparator = semanticVersioningComparator;
  }

  public List<BlockingConfigResponseElement> generateBlockingElements(
      List<BlockingConfigRequestElement> requestElements,
      RequestContext requestContext,
      Optional<String> environmentId) {
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
            requestContext,
            environmentId,
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3)
        .ifPresent(responseElements::add);
    buildResponseElement(
            customSignatureRequestElements,
            requestContext,
            environmentId,
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS)
        .ifPresent(responseElements::add);
    return responseElements;
  }

  private Optional<BlockingConfigResponseElement> buildResponseElement(
      List<BlockingConfigRequestElement> requestElements,
      RequestContext requestContext,
      Optional<String> environmentId,
      CustomModsecRuleVersion customModsecRuleVersion) {
    List<BlockingConfigRequestElement> filteredRequestElements =
        requestElements.stream()
            .filter(
                requestElement ->
                    isCustomModsecVersionSupported(requestElement, customModsecRuleVersion))
            .collect(Collectors.toUnmodifiableList());
    // We don't want to send the rules to agents if they don't require it
    if (filteredRequestElements.isEmpty()) {
      return Optional.empty();
    }

    String customSignatureRulesBlob =
        customSignatureBlobFetcher.getEnabledCustomSignatureRulesBlob(
            requestContext, customModsecRuleVersion, environmentId);
    String responseHash = uuidGenerator.generateId(customSignatureRulesBlob);

    // Getting all the libtraceable versions explicitly mentioned
    List<AgentCapabilities> libtraceableCapabilities =
        filteredRequestElements.stream()
            .flatMap(requestElement -> requestElement.getSupportedAgentCapabilitiesList().stream())
            .flatMap(agentCapabilities -> agentCapabilities.getComponentsList().stream())
            .filter(Component::hasLibtraceableVersion)
            .filter(component -> getSupportedRuleVersion(component) == customModsecRuleVersion)
            .distinct()
            .map(component -> AgentCapabilities.newBuilder().addComponents(component).build())
            .collect(Collectors.toUnmodifiableList());

    // Checking if hashes of all requests are same
    if (filteredRequestElements.stream()
        .map(BlockingConfigRequestElement::getPreviousHash)
        .allMatch(responseHash::equals)) {
      return Optional.of(
          BlockingConfigResponseElement.newBuilder()
              .setHash(responseHash)
              .addAllAgentCapabilities(libtraceableCapabilities)
              .setCustomSignatureBlockingRules(CustomSignatureBlockingRules.getDefaultInstance())
              .build());
    }

    return Optional.of(
        BlockingConfigResponseElement.newBuilder()
            .setHash(responseHash)
            .addAllAgentCapabilities(libtraceableCapabilities)
            .setCustomSignatureBlockingRules(
                CustomSignatureBlockingRules.newBuilder()
                    .setCustomSignatureRulesBlob(customSignatureRulesBlob))
            .build());
  }

  private boolean isCustomModsecVersionSupported(
      BlockingConfigRequestElement requestElement,
      CustomModsecRuleVersion customModsecRuleVersion) {
    // Concerned with only libtraceable version for returning modsec with seg args
    // TPA version can is irrelevant as the object type is string blob in both cases
    if (customModsecRuleVersion == CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3
        && requestElement.getSupportedAgentCapabilitiesList().isEmpty()) {
      return true;
    }
    return requestElement.getSupportedAgentCapabilitiesList().stream()
        .anyMatch(
            agentCapabilities ->
                isSecArgLimitsSupported(agentCapabilities, customModsecRuleVersion));
  }

  private boolean isSecArgLimitsSupported(
      AgentCapabilities agentCapabilities, CustomModsecRuleVersion customModsecRuleVersion) {
    // An agent can be said to support seg arg limit if it has any component with desired
    // libtraceable version
    return agentCapabilities.getComponentsList().stream()
        .map(this::getSupportedRuleVersion)
        .anyMatch(Predicate.isEqual(customModsecRuleVersion));
  }

  private CustomModsecRuleVersion getSupportedRuleVersion(Component component) {
    // A component can be said to support seg arg limit if it has desired libtraceable version
    if (component.hasLibtraceableVersion()
        && semanticVersioningComparator.compare(
                component.getLibtraceableVersion(), MINIMUM_LIBTRACEABLE_VERSION_FOR_V3_SECARG)
            >= 0) {
      return CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS;
    }
    return CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3;
  }
}
