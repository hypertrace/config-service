package ai.traceable.blocking.config.service.blockingpolicy;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.ActorBasedDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.CustomIpBasedDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.CustomSignatureDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.ModsecDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.RegionDataFetcher;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingPolicyConfiguration;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

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
  public BlockingPolicyConfiguration getBlockingPolicyConfiguration(String requestHash) {
    try {
      List<BlockingDetails> blockingDetailsList = getOrderedBlockingRules();
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

  private List<BlockingDetails> getOrderedBlockingRules() {
    List<BlockingDetails> customIpBasedExemption =
        customIpBasedDataFetcher.getCustomIpBasedExemptions();

    List<BlockingDetails> threatActorExemption =
        actorBasedDataFetcher.getThreatActorBasedIpExemption();

    List<BlockingDetails> customSignatureExemption =
        customSignatureDataFetcher.getCustomSignatureExemptions();

    List<BlockingDetails> customSignatureViolation =
        customSignatureDataFetcher.getCustomSignatureViolations();

    List<BlockingDetails> modsecViolation = modsecDataFetcher.getModsecViolations();

    List<BlockingDetails> customIpBasedBlockAllExcept =
        customIpBasedDataFetcher.getCustomIpBasedBlockAllExcepts();

    List<BlockingDetails> customIpBasedViolation =
        customIpBasedDataFetcher.getCustomIpBasedViolations();

    List<BlockingDetails> threatActorViolation =
        actorBasedDataFetcher.getThreatActorBasedIpViolation();

    List<BlockingDetails> regionBlockAllExcept = regionDataFetcher.getRegionViolationAllExcepts();

    List<BlockingDetails> regionViolation = regionDataFetcher.getRegionViolations();

    List<BlockingDetails> rateLimitViolation = actorBasedDataFetcher.getRateLimitBasedIpViolation();

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
