package ai.traceable.blocking.config.service.v2.modsec;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.blocking.config.service.common.modsec.BlockingModsecBlobFetcher;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.v2.AgentCapabilities;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.Component;
import ai.traceable.blocking.config.service.v2.CrsBlockingRules;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.SemanticVersioningComparator;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ModsecBlockingManager implements BlockingConfigManagerBase {

  /**
   * We basically have two variants of modsec rules - V3 and V3_Sec_Args For any agent which can
   * prove that it has a modsec engine which can handle sec_args rules we want to send the
   * V3_Sec_Args. Otherwise, we send V3. We determine support for sec_args rules if the libtraceable
   * version is greater than minimum
   */
  private static final String MINIMUM_LIBTRACEABLE_VERSION_FOR_V3_SECARG_DETECTION_ONLY =
      "0.1.98-rc.162";

  private final BlockingModsecBlobFetcher blockingModsecBlobFetcher;
  private final UuidGenerator uuidGenerator;
  private final SemanticVersioningComparator semanticVersioningComparator;
  private final FeatureCachingClient featureCachingClient;

  @Inject
  public ModsecBlockingManager(
      BlockingModsecBlobFetcher blockingModsecBlobFetcher,
      UuidGenerator uuidGenerator,
      SemanticVersioningComparator semanticVersioningComparator,
      FeatureCachingClient featureCachingClient) {
    this.blockingModsecBlobFetcher = blockingModsecBlobFetcher;
    this.uuidGenerator = uuidGenerator;
    this.semanticVersioningComparator = semanticVersioningComparator;
    this.featureCachingClient = featureCachingClient;
  }

  @Override
  public List<BlockingConfigResponseElement> generateBlockingElements(
      RequestContext requestContext,
      List<BlockingConfigRequestElement> requestElements,
      BlockingRulesSupplier blockingRulesSupplier) {
    List<AgentCapabilities> agentCapabilitiesWithEdsEnabled =
        getAgentCapabilitiesWithEdsEnabled(requestElements, requestContext);
    List<BlockingConfigRequestElement> modsecRequestElements =
        filterRequestElements(requestElements, requestContext);
    if (modsecRequestElements.isEmpty() && agentCapabilitiesWithEdsEnabled.isEmpty()) {
      return Collections.emptyList();
    }
    List<BlockingConfigResponseElement> responseElements = new ArrayList<>();
    if (!agentCapabilitiesWithEdsEnabled.isEmpty()) {
      // adding response element with empty blob hash for agents with eds enabled - this is to
      // ensure that agents with eds enabled don't evaluate modsec rules - instead eds will
      // evaluate the modsec rules
      responseElements.add(
          BlockingConfigResponseElement.newBuilder()
              .setHash(uuidGenerator.getEmptyValueUuid())
              .addAllAgentCapabilities(agentCapabilitiesWithEdsEnabled)
              .setCrsBlockingRules(CrsBlockingRules.getDefaultInstance())
              .build());
    }
    if (modsecRequestElements.isEmpty()) {
      return responseElements;
    }
    buildResponseElement(
            modsecRequestElements, ModsecRuleVersion.MODSEC_RULE_VERSION_V3, blockingRulesSupplier)
        .ifPresent(responseElements::add);
    buildResponseElement(
            modsecRequestElements,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE,
            blockingRulesSupplier)
        .ifPresent(responseElements::add);
    return responseElements;
  }

  private List<AgentCapabilities> getAgentCapabilitiesWithEdsEnabled(
      List<BlockingConfigRequestElement> requestElements, RequestContext requestContext) {
    // we send modsec rules to both libtraceable & edge if dual evaluation is enabled
    if (featureCachingClient.isProtectionBlockingDualEvaluationEnabledForTenant(requestContext)) {
      return Collections.emptyList();
    }
    return requestElements.stream()
        .filter(BlockingConfigRequestElement::hasCrsBlockingRulesRequest)
        .flatMap(requestElement -> requestElement.getSupportedAgentCapabilitiesList().stream())
        .filter(
            agentCapabilities ->
                agentCapabilities.getComponentsList().stream().anyMatch(Component::getEdsEnabled)
                    && featureCachingClient.isProtectionEngineWebAppProtectionEnabledForTenant(
                        requestContext))
        .collect(Collectors.toUnmodifiableList());
  }

  private List<BlockingConfigRequestElement> filterRequestElements(
      List<BlockingConfigRequestElement> requestElements, RequestContext requestContext) {
    // 1. filter out requests which are not for crs blocking rules
    // 2. modify request elements to remove agent capabilities which have eds enabled
    // 3. filter out requests which have no agent capabilities left
    return requestElements.stream()
        .filter(BlockingConfigRequestElement::hasCrsBlockingRulesRequest)
        .flatMap(
            requestElement -> {
              // we send modsec rules to both libtraceable & edge if dual evaluation is enabled
              if (featureCachingClient.isProtectionBlockingDualEvaluationEnabledForTenant(
                  requestContext)) {
                return Stream.of(requestElement);
              }
              List<AgentCapabilities> agentCapabilitiesWithEdsDisabled =
                  requestElement.getSupportedAgentCapabilitiesList().stream()
                      .filter(
                          agentCapabilities ->
                              agentCapabilities.getComponentsList().stream()
                                      .noneMatch(Component::getEdsEnabled)
                                  || !featureCachingClient
                                      .isProtectionEngineWebAppProtectionEnabledForTenant(
                                          requestContext))
                      .collect(Collectors.toUnmodifiableList());
              if (agentCapabilitiesWithEdsDisabled.isEmpty()) {
                return Stream.empty();
              }
              return Stream.of(
                  requestElement.toBuilder()
                      .clearSupportedAgentCapabilities()
                      .addAllSupportedAgentCapabilities(agentCapabilitiesWithEdsDisabled)
                      .build());
            })
        .collect(Collectors.toUnmodifiableList());
  }

  private Optional<BlockingConfigResponseElement> buildResponseElement(
      List<BlockingConfigRequestElement> requestElements,
      ModsecRuleVersion modsecRuleVersion,
      BlockingRulesSupplier blockingRulesSupplier) {
    List<AgentCapabilities> matchingRequestAgentCapabilities = new ArrayList<>();
    List<String> previousHashes = new ArrayList<>();
    requestElements.forEach(
        requestElement -> {
          List<AgentCapabilities> filteredAgentCapabilities =
              requestElement.getSupportedAgentCapabilitiesList().stream()
                  .filter(
                      agentCapabilities ->
                          this.isModsecRuleVersionSupported(agentCapabilities, modsecRuleVersion))
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

    String blockingCrsRulesBlob =
        blockingModsecBlobFetcher.getEnabledRulesBlob(
            blockingRulesSupplier.getRequestContext(),
            modsecRuleVersion,
            blockingRulesSupplier.getEnvironmentId());
    String responseHash = uuidGenerator.generateId(blockingCrsRulesBlob);

    // Checking if hashes of all requests are same
    if (previousHashes.stream().allMatch(responseHash::equals)) {
      return Optional.of(
          BlockingConfigResponseElement.newBuilder()
              .setHash(responseHash)
              .addAllAgentCapabilities(matchingRequestAgentCapabilities)
              .setCrsBlockingRules(CrsBlockingRules.getDefaultInstance())
              .build());
    }

    return Optional.of(
        BlockingConfigResponseElement.newBuilder()
            .setHash(responseHash)
            .addAllAgentCapabilities(matchingRequestAgentCapabilities)
            .setCrsBlockingRules(
                CrsBlockingRules.newBuilder().setCrsRulesBlob(blockingCrsRulesBlob))
            .build());
  }

  private boolean isModsecRuleVersionSupported(
      AgentCapabilities agentCapabilities, ModsecRuleVersion modsecRuleVersion) {
    // An agent can be said to support seg arg limit if it has any component with desired
    // libtraceable version
    return (modsecRuleVersion.equals(
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE))
        ? agentCapabilities.getComponentsList().stream().anyMatch(this::isSecArgLimitsSupported)
        : agentCapabilities.getComponentsList().stream().noneMatch(this::isSecArgLimitsSupported);
  }

  private boolean isSecArgLimitsSupported(Component component) {
    // A component can be said to support seg arg limit if it has desired libtraceable version
    return component.hasLibtraceableVersion()
        && semanticVersioningComparator.compare(
                component.getLibtraceableVersion(),
                MINIMUM_LIBTRACEABLE_VERSION_FOR_V3_SECARG_DETECTION_ONLY)
            >= 0;
  }
}
