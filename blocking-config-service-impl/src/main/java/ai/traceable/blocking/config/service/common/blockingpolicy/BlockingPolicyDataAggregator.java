package ai.traceable.blocking.config.service.common.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.ActorBasedDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.CustomIpBasedDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.CustomSignatureDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.ModsecDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.RegionDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.ActorBasedRulesCollection;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.MaliciousSourcesDataFetcher;
import com.google.inject.Inject;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.grpcutils.context.RequestContext;

class BlockingPolicyDataAggregator {
  private final ActorBasedDataFetcher actorBasedDataFetcher;
  private final CustomIpBasedDataFetcher customIpBasedDataFetcher;
  private final CustomSignatureDataFetcher customSignatureDataFetcher;
  private final ModsecDataFetcher modsecDataFetcher;
  private final RegionDataFetcher regionDataFetcher;
  private final MaliciousSourcesDataFetcher maliciousSourceRuleDataFetcher;

  @Inject
  BlockingPolicyDataAggregator(
      ActorBasedDataFetcher actorBasedDataFetcher,
      CustomIpBasedDataFetcher customIpBasedDataFetcher,
      CustomSignatureDataFetcher customSignatureDataFetcher,
      ModsecDataFetcher modsecDataFetcher,
      RegionDataFetcher regionDataFetcher,
      MaliciousSourcesDataFetcher maliciousSourceRuleDataFetcher) {
    this.actorBasedDataFetcher = actorBasedDataFetcher;
    this.customIpBasedDataFetcher = customIpBasedDataFetcher;
    this.customSignatureDataFetcher = customSignatureDataFetcher;
    this.modsecDataFetcher = modsecDataFetcher;
    this.regionDataFetcher = regionDataFetcher;
    this.maliciousSourceRuleDataFetcher = maliciousSourceRuleDataFetcher;
  }

  List<BlockingPolicyData> getOrderedBlockingRules(
      RequestContext requestContext, Optional<String> environmentId) {
    // Actor based rules
    ActorBasedRulesCollection actorBasedRulesCollection =
        actorBasedDataFetcher.getActorBasedRules(requestContext, environmentId);
    List<BlockingPolicyData> threatActorExemptions =
        actorBasedRulesCollection.getThreatActorBasedIpExemptions();
    List<BlockingPolicyData> threatActorViolations =
        actorBasedRulesCollection.getThreatActorBasedIpViolations();
    List<BlockingPolicyData> rateLimitViolations =
        actorBasedRulesCollection.getRateLimitBasedIpViolations();
    List<BlockingPolicyData> emailDomainBasedExemptions =
        actorBasedRulesCollection.getEmailDomainBasedExemptions();
    List<BlockingPolicyData> emailDomainBasedViolations =
        actorBasedRulesCollection.getEmailDomainBasedViolations();

    // Custom signature rules
    Map<RuleType, List<BlockingPolicyData>> customSignatureRulesMap =
        customSignatureDataFetcher.getCustomSignatureRules(requestContext, environmentId);
    List<BlockingPolicyData> customSignatureExemptions =
        customSignatureRulesMap.get(RuleType.ALLOW);
    List<BlockingPolicyData> customSignatureViolations =
        customSignatureRulesMap.get(RuleType.BLOCK);

    // Modsec
    List<BlockingPolicyData> modsecViolations =
        modsecDataFetcher.getModsecViolations(requestContext, environmentId);

    // Custom ip rules
    Map<RuleType, List<BlockingPolicyData>> customIpBasedRulesMap =
        customIpBasedDataFetcher.getCustomIpBasedRules(requestContext, environmentId);
    List<BlockingPolicyData> customIpBasedExemptions = customIpBasedRulesMap.get(RuleType.ALLOW);
    List<BlockingPolicyData> customIpBasedBlockAllExcepts =
        customIpBasedRulesMap.get(RuleType.BLOCK_ALL_EXCEPT);
    List<BlockingPolicyData> customIpBasedViolations = customIpBasedRulesMap.get(RuleType.BLOCK);

    // Region based rules
    Map<RuleType, List<BlockingPolicyData>> regionBasedRulesMap =
        regionDataFetcher.getRegionBasedRules(requestContext, environmentId);
    List<BlockingPolicyData> regionBlockAllExcepts =
        regionBasedRulesMap.get(RuleType.BLOCK_ALL_EXCEPT);
    List<BlockingPolicyData> regionViolations = regionBasedRulesMap.get(RuleType.BLOCK);

    Map<BlockingPolicyDataBucket, List<BlockingPolicyData>> maliciousSourcesRulesBlockingData =
        maliciousSourceRuleDataFetcher.getMaliciousSourceRuleBlockingDetails(
            requestContext, environmentId);
    // TODO: cleaner way to enforce ordering
    /*
     Rules follow the precedence mentioned here
     https://traceableai.atlassian.net/wiki/spaces/Engineering/pages/1265139838/Blocking+rules+-+evaluation+order+of+precedence
    Current order -
            "malicious-source-ip-range-exemption",
                "custom-ip-based-exemption",
                "threat-actor-exemption",
                "email-domain-exemption",
                "custom-signature-exemption",
                "custom-signature-violation",
                "modsec-violation",
                "malicious-source-ip-range-block-all-except",
                "custom-ip-based-block-all-except",
                "malicious-source-ip-range-violation",
                "custom-ip-based-violation",
                "threat-actor-violation",
                "email-domain-violation",
                "malicious-source-ip-type-violation",
                "malicious-source-region-block-all-except",
                "region-block-all-except",
                "malicious-source-region-violation",
                "region-violation",
                "rate-limit-violation"
       */
    return Stream.of(
            maliciousSourcesRulesBlockingData.getOrDefault(
                BlockingPolicyDataBucket.IP_RANGE_EXEMPTIONS, Collections.emptyList()),
            customIpBasedExemptions,
            threatActorExemptions,
            emailDomainBasedExemptions,
            customSignatureExemptions,
            customSignatureViolations,
            modsecViolations,
            maliciousSourcesRulesBlockingData.getOrDefault(
                BlockingPolicyDataBucket.IP_RANGE_BLOCK_ALL_EXCEPT_VIOLATIONS,
                Collections.emptyList()),
            customIpBasedBlockAllExcepts,
            maliciousSourcesRulesBlockingData.getOrDefault(
                BlockingPolicyDataBucket.IP_RANGE_VIOLATIONS, Collections.emptyList()),
            customIpBasedViolations,
            threatActorViolations,
            emailDomainBasedViolations,
            maliciousSourcesRulesBlockingData.getOrDefault(
                BlockingPolicyDataBucket.IP_TYPE_VIOLATIONS, Collections.emptyList()),
            maliciousSourcesRulesBlockingData.getOrDefault(
                BlockingPolicyDataBucket.REGION_BLOCK_ALL_EXCEPT_VIOLATIONS,
                Collections.emptyList()),
            regionBlockAllExcepts,
            maliciousSourcesRulesBlockingData.getOrDefault(
                BlockingPolicyDataBucket.REGION_VIOLATIONS, Collections.emptyList()),
            regionViolations,
            rateLimitViolations)
        .flatMap(Collection::stream)
        .collect(Collectors.toUnmodifiableList());
  }
}
