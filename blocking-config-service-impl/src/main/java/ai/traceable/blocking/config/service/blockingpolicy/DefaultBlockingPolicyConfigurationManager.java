package ai.traceable.blocking.config.service.blockingpolicy;

import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.ActorBasedDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.CustomIpBasedDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.CustomSignatureDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.IpTypeDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.ModsecDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.RegionDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor.ActorBasedRulesCollection;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingPolicyConfiguration;
import ai.traceable.blocking.config.service.v1.BlockingRuleType;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DefaultBlockingPolicyConfigurationManager
    implements BlockingPolicyConfigurationManager {
  private final ActorBasedDataFetcher actorBasedDataFetcher;
  private final CustomIpBasedDataFetcher customIpBasedDataFetcher;
  private final CustomSignatureDataFetcher customSignatureDataFetcher;
  private final ModsecDataFetcher modsecDataFetcher;
  private final RegionDataFetcher regionDataFetcher;
  private final IpTypeDataFetcher ipTypeDataFetcher;
  private final UuidGenerator uuidGenerator;

  @Inject
  public DefaultBlockingPolicyConfigurationManager(
      ActorBasedDataFetcher actorBasedDataFetcher,
      CustomIpBasedDataFetcher customIpBasedDataFetcher,
      CustomSignatureDataFetcher customSignatureDataFetcher,
      ModsecDataFetcher modsecDataFetcher,
      RegionDataFetcher regionDataFetcher,
      IpTypeDataFetcher ipTypeDataFetcher,
      UuidGenerator uuidGenerator) {
    this.actorBasedDataFetcher = actorBasedDataFetcher;
    this.customIpBasedDataFetcher = customIpBasedDataFetcher;
    this.customSignatureDataFetcher = customSignatureDataFetcher;
    this.modsecDataFetcher = modsecDataFetcher;
    this.regionDataFetcher = regionDataFetcher;
    this.ipTypeDataFetcher = ipTypeDataFetcher;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public BlockingPolicyConfiguration getBlockingPolicyConfiguration(
      RequestContext requestContext, String requestHash, Optional<String> environmentId) {
    try {
      List<BlockingDetails> blockingDetailsList =
          getOrderedBlockingRules(requestContext, environmentId);
      String responseHash = uuidGenerator.generateId(blockingDetailsList);
      if (responseHash.equals(requestHash)) {
        return BlockingPolicyConfiguration.newBuilder().setHash(requestHash).build();
      }
      return BlockingPolicyConfiguration.newBuilder()
          .setHash(responseHash)
          .addAllBlockingDetailsList(blockingDetailsList)
          .build();
    } catch (Exception e) {
      log.error(
          "Unable to create blocking policy configuration for tenant:{} ",
          requestContext.getTenantId(),
          e);
    }
    return BlockingPolicyConfiguration.newBuilder().setHash(requestHash).build();
  }

  private List<BlockingDetails> getOrderedBlockingRules(
      RequestContext requestContext, Optional<String> environmentId) {
    // Actor based rules
    ActorBasedRulesCollection actorBasedRulesCollection =
        actorBasedDataFetcher.getActorBasedRules(requestContext, environmentId);
    List<BlockingDetails> threatActorExemptions =
        actorBasedRulesCollection.getThreatActorBasedIpExemptions();
    List<BlockingDetails> threatActorViolations =
        actorBasedRulesCollection.getThreatActorBasedIpViolations();
    List<BlockingDetails> rateLimitViolations =
        actorBasedRulesCollection.getRateLimitBasedIpViolations();
    List<BlockingDetails> emailDomainBasedExemptions =
        actorBasedRulesCollection.getEmailDomainBasedExemptions();
    List<BlockingDetails> emailDomainBasedViolations =
        actorBasedRulesCollection.getEmailDomainBasedViolations();

    // Custom signature rules
    Map<BlockingRuleType, List<BlockingDetails>> customSignatureRulesMap =
        customSignatureDataFetcher.getCustomSignatureRules(requestContext, environmentId);
    List<BlockingDetails> customSignatureExemptions =
        customSignatureRulesMap.get(BLOCKING_RULE_TYPE_ALLOW);
    List<BlockingDetails> customSignatureViolations =
        customSignatureRulesMap.get(BLOCKING_RULE_TYPE_BLOCK);

    // Modsec
    List<BlockingDetails> modsecViolations =
        modsecDataFetcher.getModsecViolations(requestContext, environmentId);

    // Custom ip rules
    Map<BlockingRuleType, List<BlockingDetails>> customIpBasedRulesMap =
        customIpBasedDataFetcher.getCustomIpBasedRules(requestContext, environmentId);
    List<BlockingDetails> customIpBasedExemptions =
        customIpBasedRulesMap.get(BLOCKING_RULE_TYPE_ALLOW);
    List<BlockingDetails> customIpBasedBlockAllExcepts =
        customIpBasedRulesMap.get(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT);
    List<BlockingDetails> customIpBasedViolations =
        customIpBasedRulesMap.get(BLOCKING_RULE_TYPE_BLOCK);

    // Region based rules
    Map<BlockingRuleType, List<BlockingDetails>> regionBasedRulesMap =
        regionDataFetcher.getRegionBasedRules(requestContext, environmentId);
    List<BlockingDetails> regionBlockAllExcepts =
        regionBasedRulesMap.get(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT);
    List<BlockingDetails> regionViolations = regionBasedRulesMap.get(BLOCKING_RULE_TYPE_BLOCK);

    // Ip-type
    List<BlockingDetails> ipTypeViolations =
        ipTypeDataFetcher.getIpTypeViolations(requestContext, environmentId);

    // TODO: cleaner way to enforce ordering
    /*
     Rules follow the precedence mentioned here
     https://traceableai.atlassian.net/wiki/spaces/Engineering/pages/1265139838/Blocking+rules+-+evaluation+order+of+precedence
    Current order -
            "custom-ip-based-exemption",
            "threat-actor-exemption",
            "email-domain-exemption",
            "custom-signature-exemption",
            "custom-signature-violation",
            "modsec-violation",
            "custom-ip-based-block-all-except",
            "custom-ip-based-violation",
            "threat-actor-violation",
            "email-domain-violation",
            "ip-type-violation",
            "region-block-all-except",
            "region-violation",
            "rate-limit-violation"
       */
    return Stream.of(
            customIpBasedExemptions,
            threatActorExemptions,
            emailDomainBasedExemptions,
            customSignatureExemptions,
            customSignatureViolations,
            modsecViolations,
            customIpBasedBlockAllExcepts,
            customIpBasedViolations,
            threatActorViolations,
            emailDomainBasedViolations,
            ipTypeViolations,
            regionBlockAllExcepts,
            regionViolations,
            rateLimitViolations)
        .flatMap(Collection::stream)
        .collect(Collectors.toUnmodifiableList());
  }
}
