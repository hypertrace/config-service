package ai.traceable.blocking.config.service.v2.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingDetailsVisitor;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CombinationBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CombinationBlockingDetails.Operator;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CustomSignatureBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpTypeBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.ModsecBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.RegionBlockingDetails;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCombination;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCombination.ConditionsOperator;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCondition;
import ai.traceable.blocking.config.service.v2.CustomSignatureDetails;
import ai.traceable.blocking.config.service.v2.IpDetails;
import ai.traceable.blocking.config.service.v2.IpType;
import ai.traceable.blocking.config.service.v2.IpTypeDetails;
import ai.traceable.blocking.config.service.v2.ModsecDetails;
import ai.traceable.blocking.config.service.v2.RegionDetails;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import java.util.stream.Collectors;

abstract class AbstractBlockingDetailsVisitor
    implements BlockingDetailsVisitor<BlockingDetailsCondition> {

  @Override
  public BlockingDetailsCondition visit(CombinationBlockingDetails combinationBlockingDetails) {
    return BlockingDetailsCondition.newBuilder()
        .setDetailsCombination(
            BlockingDetailsCombination.newBuilder()
                .setOperator(getOperator(combinationBlockingDetails.getOperator()))
                .addAllDetailsConditions(
                    combinationBlockingDetails.getBlockingDetailsOperands().stream()
                        .map(blockingDetails -> blockingDetails.accept(this))
                        .collect(Collectors.toUnmodifiableList())))
        .build();
  }

  private static ConditionsOperator getOperator(Operator operator) {
    switch (operator) {
      case AND:
        return ConditionsOperator.CONDITIONS_OPERATOR_AND;
      case OR:
        return ConditionsOperator.CONDITIONS_OPERATOR_OR;
      case NOT:
        return ConditionsOperator.CONDITIONS_OPERATOR_NOT;
    }
    throw new UnsupportedOperationException(String.format("Unsupported Operator %s", operator));
  }

  @Override
  public BlockingDetailsCondition visit(
      CustomSignatureBlockingDetails customSignatureBlockingDetails) {
    if (customSignatureBlockingDetails.getRuleIds().size() == 1) {
      return BlockingDetailsCondition.newBuilder()
          .setCustomSignatureDetails(
              CustomSignatureDetails.newBuilder()
                  .setRuleId(customSignatureBlockingDetails.getRuleIds().get(0)))
          .build();
    }
    return BlockingDetailsCondition.newBuilder()
        .setDetailsCombination(
            BlockingDetailsCombination.newBuilder()
                .setOperator(ConditionsOperator.CONDITIONS_OPERATOR_OR)
                .addAllDetailsConditions(
                    customSignatureBlockingDetails.getRuleIds().stream()
                        .map(
                            ruleId ->
                                BlockingDetailsCondition.newBuilder()
                                    .setCustomSignatureDetails(
                                        CustomSignatureDetails.newBuilder().setRuleId(ruleId))
                                    .build())
                        .collect(Collectors.toUnmodifiableList())))
        .build();
  }

  @Override
  public BlockingDetailsCondition visit(IpBlockingDetails ipBlockingDetails) {
    return BlockingDetailsCondition.newBuilder()
        .setIpDetails(
            IpDetails.newBuilder()
                .addAllIpAddresses(ipBlockingDetails.getIpAddresses())
                .addAllIpRanges(ipBlockingDetails.getIpRanges()))
        .build();
  }

  @Override
  public BlockingDetailsCondition visit(IpTypeBlockingDetails ipTypeBlockingDetails) {
    return BlockingDetailsCondition.newBuilder()
        .setIpTypeDetails(
            IpTypeDetails.newBuilder()
                .addAllIpTypes(
                    ipTypeBlockingDetails.getIpTypes().stream()
                        .map(AbstractBlockingDetailsVisitor::convert)
                        .collect(Collectors.toUnmodifiableList())))
        .build();
  }

  @Override
  public BlockingDetailsCondition visit(ModsecBlockingDetails modsecBlockingDetails) {
    if (modsecBlockingDetails.getRuleIds().size() == 1) {
      return BlockingDetailsCondition.newBuilder()
          .setModsecDetails(
              ModsecDetails.newBuilder().setRuleId(modsecBlockingDetails.getRuleIds().get(0)))
          .build();
    }
    return BlockingDetailsCondition.newBuilder()
        .setDetailsCombination(
            BlockingDetailsCombination.newBuilder()
                .setOperator(ConditionsOperator.CONDITIONS_OPERATOR_OR)
                .addAllDetailsConditions(
                    modsecBlockingDetails.getRuleIds().stream()
                        .map(
                            ruleId ->
                                BlockingDetailsCondition.newBuilder()
                                    .setModsecDetails(ModsecDetails.newBuilder().setRuleId(ruleId))
                                    .build())
                        .collect(Collectors.toUnmodifiableList())))
        .build();
  }

  @Override
  public BlockingDetailsCondition visit(RegionBlockingDetails regionBlockingDetails) {
    return BlockingDetailsCondition.newBuilder()
        .setRegionDetails(
            RegionDetails.newBuilder().addAllRegions(regionBlockingDetails.getRegions()))
        .build();
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
