package ai.traceable.blocking.config.service.v2.modsec;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.blocking.config.service.common.modsec.BlockingModsecBlobFetcher;
import ai.traceable.blocking.config.service.common.util.SemanticVersioningComparator;
import ai.traceable.blocking.config.service.v2.AgentCapabilities;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.Component;
import ai.traceable.blocking.config.service.v2.CrsBlockingRules;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ModsecBlockingManager implements BlockingConfigManagerBase {

  /**
   * We basically have two variants of modsec rules - V3 and V3_Sec_Args For any agent which can
   * prove that it has a modsec engine which can handle sec_args rules we want to send the
   * V3_Sec_Args. Otherwise, we send V3. We determine support for sec_args rules if the libtraceable
   * version is greater than minimum
   */
  private static final String MINIMUM_LIBTRACEABLE_VERSION_FOR_V3_SECARG = "0.1.98-rc.139";

  private final BlockingModsecBlobFetcher blockingModsecBlobFetcher;
  private final UuidGenerator uuidGenerator;
  private final SemanticVersioningComparator semanticVersioningComparator;

  @Inject
  public ModsecBlockingManager(
      BlockingModsecBlobFetcher blockingModsecBlobFetcher,
      UuidGenerator uuidGenerator,
      SemanticVersioningComparator semanticVersioningComparator) {
    this.blockingModsecBlobFetcher = blockingModsecBlobFetcher;
    this.uuidGenerator = uuidGenerator;
    this.semanticVersioningComparator = semanticVersioningComparator;
  }

  public List<BlockingConfigResponseElement> generateBlockingElements(
      List<BlockingConfigRequestElement> requestElements,
      RequestContext requestContext,
      Optional<String> environmentId) {
    List<BlockingConfigRequestElement> modsecRequestElements =
        requestElements.stream()
            .filter(BlockingConfigRequestElement::hasCrsBlockingRulesRequest)
            .collect(Collectors.toUnmodifiableList());
    if (modsecRequestElements.isEmpty()) {
      return Collections.emptyList();
    }

    List<BlockingConfigResponseElement> responseElements = new ArrayList<>();
    buildResponseElement(modsecRequestElements, ModsecRuleVersion.MODSEC_RULE_VERSION_V3)
        .ifPresent(responseElements::add);
    buildResponseElement(
            modsecRequestElements, ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS)
        .ifPresent(responseElements::add);
    return responseElements;
  }

  private Optional<BlockingConfigResponseElement> buildResponseElement(
      List<BlockingConfigRequestElement> requestElements, ModsecRuleVersion modsecRuleVersion) {
    List<BlockingConfigRequestElement> filteredRequestElements =
        requestElements.stream()
            .filter(requestElement -> isModsecVersionSupported(requestElement, modsecRuleVersion))
            .collect(Collectors.toUnmodifiableList());

    // We don't want to send the rules to agents if they don't require it
    if (filteredRequestElements.isEmpty()) {
      return Optional.empty();
    }

    String blockingCrsRulesBlob =
        blockingModsecBlobFetcher.getBlockingModsecBlob(modsecRuleVersion);
    String responseHash = uuidGenerator.generateId(blockingCrsRulesBlob);

    // Getting all the libtraceable versions explicitly mentioned
    List<AgentCapabilities> libtraceableCapabilities =
        filteredRequestElements.stream()
            .flatMap(requestElement -> requestElement.getSupportedAgentCapabilitiesList().stream())
            .flatMap(agentCapabilities -> agentCapabilities.getComponentsList().stream())
            .filter(Predicate.not(Component::hasTraceablePlatformAgentVersion))
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
              .setCrsBlockingRules(CrsBlockingRules.getDefaultInstance())
              .build());
    }

    return Optional.of(
        BlockingConfigResponseElement.newBuilder()
            .setHash(responseHash)
            .addAllAgentCapabilities(libtraceableCapabilities)
            .setCrsBlockingRules(
                CrsBlockingRules.newBuilder().setCrsRulesBlob(blockingCrsRulesBlob))
            .build());
  }

  private boolean isModsecVersionSupported(
      BlockingConfigRequestElement requestElement, ModsecRuleVersion modsecRuleVersion) {
    Predicate<AgentCapabilities> checker = this::isSecArgLimitsSupported;
    if (modsecRuleVersion == ModsecRuleVersion.MODSEC_RULE_VERSION_V3) {
      // By default send v3
      if (requestElement.getSupportedAgentCapabilitiesList().isEmpty()) {
        return true;
      }
      checker = Predicate.not(checker);
    }

    // Concerned with only libtraceable version for returning modsec with seg args
    // TPA version can is irrelevant as the object type is string blob in both cases
    return requestElement.getSupportedAgentCapabilitiesList().stream().anyMatch(checker);
  }

  private boolean isSecArgLimitsSupported(AgentCapabilities agentCapabilities) {
    // An agent can be said to support seg arg limit if it has any component with desired
    // libtraceable version
    return agentCapabilities.getComponentsList().stream().anyMatch(this::isSecArgLimitsSupported);
  }

  private boolean isSecArgLimitsSupported(Component component) {
    // A component can be said to support seg arg limit if it has desired libtraceable version
    return component.hasLibtraceableVersion()
        && semanticVersioningComparator.compare(
                component.getLibtraceableVersion(), MINIMUM_LIBTRACEABLE_VERSION_FOR_V3_SECARG)
            >= 0;
  }
}
