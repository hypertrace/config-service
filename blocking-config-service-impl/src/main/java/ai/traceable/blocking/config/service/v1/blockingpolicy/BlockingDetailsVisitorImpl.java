package ai.traceable.blocking.config.service.v1.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.ActorBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingDetailsVisitor;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CombinationBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CustomSignatureBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpTypeBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.ModsecBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.RegionBlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingDetails.Builder;
import ai.traceable.blocking.config.service.v1.CustomSignatureDetails;
import ai.traceable.blocking.config.service.v1.IpDetails;
import ai.traceable.blocking.config.service.v1.IpType;
import ai.traceable.blocking.config.service.v1.IpTypeDetails;
import ai.traceable.blocking.config.service.v1.ModsecDetails;
import ai.traceable.blocking.config.service.v1.RegionDetails;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BlockingDetailsVisitorImpl implements BlockingDetailsVisitor<Builder> {
  @Override
  public Builder visit(ActorBlockingDetails actorBlockingDetails) {
    return BlockingDetails.newBuilder()
        .setIpDetails(
            IpDetails.newBuilder().addAllIpAddresses(actorBlockingDetails.getIpAddresses()));
  }

  @Override
  public Builder visit(CombinationBlockingDetails combinationBlockingDetails) {
    log.warn(
        "Skipping: Blocking Config Service V1 does not support complex blocking - {}",
        combinationBlockingDetails);
    return null;
  }

  @Override
  public Builder visit(CustomSignatureBlockingDetails customSignatureBlockingDetails) {
    if (customSignatureBlockingDetails.getRuleIds().size() != 1) {
      log.warn(
          "Skipping: Blocking Config Service V1 does not support multiple custom signature rules in one blocking rule - {}",
          customSignatureBlockingDetails);
    }
    return BlockingDetails.newBuilder()
        .setCustomSignatureDetails(
            CustomSignatureDetails.newBuilder()
                .setRuleId(customSignatureBlockingDetails.getRuleIds().get(0)));
  }

  @Override
  public Builder visit(IpBlockingDetails ipBlockingDetails) {
    return BlockingDetails.newBuilder()
        .setIpDetails(
            IpDetails.newBuilder()
                .addAllIpAddresses(ipBlockingDetails.getIpAddresses())
                .addAllIpRanges(ipBlockingDetails.getIpRanges()));
  }

  @Override
  public Builder visit(IpTypeBlockingDetails ipTypeBlockingDetails) {
    return BlockingDetails.newBuilder()
        .setIpTypeDetails(
            IpTypeDetails.newBuilder()
                .addAllIpTypes(
                    ipTypeBlockingDetails.getIpTypes().stream()
                        .map(BlockingDetailsVisitorImpl::convert)
                        .collect(Collectors.toUnmodifiableList())));
  }

  @Override
  public Builder visit(ModsecBlockingDetails modsecBlockingDetails) {
    if (modsecBlockingDetails.getRuleIds().size() != 1) {
      log.warn(
          "Skipping: Blocking Config Service V1 does not support multiple modsec rules in one blocking rule - {}",
          modsecBlockingDetails);
    }
    return BlockingDetails.newBuilder()
        .setModsecDetails(
            ModsecDetails.newBuilder().setRuleId(modsecBlockingDetails.getRuleIds().get(0)));
  }

  @Override
  public Builder visit(RegionBlockingDetails regionBlockingDetails) {
    return BlockingDetails.newBuilder()
        .setRegionDetails(
            RegionDetails.newBuilder().addAllRegions(regionBlockingDetails.getRegions()));
  }

  private static IpType convert(IpLocationType ipLocationType) {
    switch (ipLocationType) {
      case IP_LOCATION_TYPE_ANONYMOUS_VPN:
        return IpType.IP_TYPE_VPN;
      case IP_LOCATION_TYPE_HOSTING_PROVIDER:
        return IpType.IP_TYPE_HOSTING_PROVIDER;
      case IP_LOCATION_TYPE_PUBLIC_PROXY:
        return IpType.IP_TYPE_PROXY;
      case IP_LOCATION_TYPE_TOR_EXIT_NODE:
        return IpType.IP_TYPE_TOR;
      case IP_LOCATION_TYPE_BOT:
        return IpType.IP_TYPE_BOT;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot convert ipLocationType %s to blocking-config-v2 rule", ipLocationType));
    }
  }
}
