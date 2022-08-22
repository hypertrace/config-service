package ai.traceable.blocking.config.service.blockingpolicy;

import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.ActorBasedDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.CustomIpBasedDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.CustomSignatureDataFetcher;
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
  private final UuidGenerator uuidGenerator;

  @Inject
  public DefaultBlockingPolicyConfigurationManager(
      ActorBasedDataFetcher actorBasedDataFetcher,
      CustomIpBasedDataFetcher customIpBasedDataFetcher,
      CustomSignatureDataFetcher customSignatureDataFetcher,
      ModsecDataFetcher modsecDataFetcher,
      RegionDataFetcher regionDataFetcher,
      UuidGenerator uuidGenerator) {
    this.actorBasedDataFetcher = actorBasedDataFetcher;
    this.customIpBasedDataFetcher = customIpBasedDataFetcher;
    this.customSignatureDataFetcher = customSignatureDataFetcher;
    this.modsecDataFetcher = modsecDataFetcher;
    this.regionDataFetcher = regionDataFetcher;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public BlockingPolicyConfiguration getBlockingPolicyConfiguration(
      RequestContext requestContext, String requestHash) {
    try {
      List<BlockingDetails> blockingDetailsList = getOrderedBlockingRules(requestContext);
      String responseHash = uuidGenerator.generateId(blockingDetailsList);
      if (responseHash.equals(requestHash)) {
        return BlockingPolicyConfiguration.newBuilder().setHash(requestHash).build();
      }
      return BlockingPolicyConfiguration.newBuilder()
          .setHash(responseHash)
          .addAllBlockingDetailsList(blockingDetailsList)
          .build();
    } catch (Exception e) {
      log.error("Unable to create blocking policy configuration", e);
    }
    return BlockingPolicyConfiguration.newBuilder().setHash(requestHash).build();
  }

  private List<BlockingDetails> getOrderedBlockingRules(RequestContext requestContext) {
    // Actor based rules
    ActorBasedRulesCollection actorBasedRulesCollection =
        actorBasedDataFetcher.getActorBasedRules(requestContext);
    List<BlockingDetails> threatActorExemption =
        actorBasedRulesCollection.getThreatActorBasedIpExemptions();
    List<BlockingDetails> threatActorViolation =
        actorBasedRulesCollection.getThreatActorBasedIpViolations();
    List<BlockingDetails> rateLimitViolation =
        actorBasedRulesCollection.getRateLimitBasedIpViolations();

    // Custom signature rules
    Map<BlockingRuleType, List<BlockingDetails>> customSignatureRulesMap =
        customSignatureDataFetcher.getCustomSignatureRules(requestContext);
    List<BlockingDetails> customSignatureExemption =
        customSignatureRulesMap.get(BLOCKING_RULE_TYPE_ALLOW);
    List<BlockingDetails> customSignatureViolation =
        customSignatureRulesMap.get(BLOCKING_RULE_TYPE_BLOCK);

    List<BlockingDetails> modsecViolation = modsecDataFetcher.getModsecViolations(requestContext);

    // Custom ip rules
    Map<BlockingRuleType, List<BlockingDetails>> customIpBasedRulesMap =
        customIpBasedDataFetcher.getCustomIpBasedRules(requestContext);
    List<BlockingDetails> customIpBasedExemption =
        customIpBasedRulesMap.get(BLOCKING_RULE_TYPE_ALLOW);
    List<BlockingDetails> customIpBasedBlockAllExcept =
        customIpBasedRulesMap.get(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT);
    List<BlockingDetails> customIpBasedViolation =
        customIpBasedRulesMap.get(BLOCKING_RULE_TYPE_BLOCK);

    // Region based rules
    Map<BlockingRuleType, List<BlockingDetails>> regionBasedRulesMap =
        regionDataFetcher.getRegionBasedRules(requestContext);
    List<BlockingDetails> regionBlockAllExcept =
        regionBasedRulesMap.get(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT);
    List<BlockingDetails> regionViolation = regionBasedRulesMap.get(BLOCKING_RULE_TYPE_BLOCK);

    // TODO: cleaner way to enforce ordering
    /*
     Rules follow the precedence mentioned here
     https://traceableai.atlassian.net/wiki/spaces/Engineering/pages/1265139838/Blocking+rules+-+evaluation+order+of+precedence
    Current order -
            "custom-ip-based-exemption",
            "threat-actor-exemption",
            "custom-signature-exemption",
            "custom-signature-violation",
            "modsec-violation",
            "custom-ip-based-block-all-except",
            "custom-ip-based-violation",
            "threat-actor-violation",
            "region-block-all-except",
            "region-violation",
            "rate-limit-violation"
       */
    return Stream.of(
            customIpBasedExemption,
            threatActorExemption,
            customSignatureExemption,
            customSignatureViolation,
            modsecViolation,
            customIpBasedBlockAllExcept,
            customIpBasedViolation,
            threatActorViolation,
            regionBlockAllExcept,
            regionViolation,
            rateLimitViolation)
        .flatMap(Collection::stream)
        .collect(Collectors.toUnmodifiableList());
  }
}
