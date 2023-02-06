package ai.traceable.blocking.config.service.v1.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.Status;
import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator.BlockingDetailsConverterBase;
import ai.traceable.blocking.config.service.v1.ActorDetails;
import ai.traceable.blocking.config.service.v1.BlockingCategory;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingDetails.Builder;
import ai.traceable.blocking.config.service.v1.BlockingRuleType;
import ai.traceable.blocking.config.service.v1.BlockingStatus;
import ai.traceable.blocking.config.service.v1.CustomSignatureDetails;
import ai.traceable.blocking.config.service.v1.IpDetails;
import ai.traceable.blocking.config.service.v1.IpTypeDetails;
import ai.traceable.blocking.config.service.v1.ModsecDetails;
import ai.traceable.blocking.config.service.v1.RegionDetails;
import ai.traceable.blocking.config.service.v1.iptype.IpTypeConverter;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class BlockingDetailsConverter implements BlockingDetailsConverterBase<BlockingDetails> {
  @Override
  public List<BlockingDetails> convert(BlockingPolicyData blockingPolicyData) {
    Builder blockingDetailsBuilder =
        BlockingDetails.newBuilder()
            .setBlockingRuleType(getBlockingRuleType(blockingPolicyData.getRuleType()))
            .setInfo(blockingPolicyData.getInfo())
            .setStatus(getBlockingStatus(blockingPolicyData.getStatus()));
    if (blockingPolicyData.getTimestamp() != 0) {
      blockingDetailsBuilder.setExpirationTimestamp(blockingPolicyData.getTimestamp());
    }

    BlockingDetails actorDetails;
    // Setting rule details based on category
    switch (blockingPolicyData.getCategory()) {
      case THREAT_ACTOR:
        blockingDetailsBuilder.setCategory(BlockingCategory.BLOCKING_CATEGORY_THREAT_ACTOR);
        blockingDetailsBuilder.setActorDetails(
            ActorDetails.newBuilder()
                .addAllIpAddresses(blockingPolicyData.getIpAddresses())
                .setUserId(blockingPolicyData.getUserId()));
        actorDetails = blockingDetailsBuilder.build();
        // for backward compatibility for agent version 1.24
        blockingDetailsBuilder
            .clearActorDetails()
            .setIpDetails(
                IpDetails.newBuilder().addAllIpAddresses(blockingPolicyData.getIpAddresses()));
        return List.of(actorDetails, blockingDetailsBuilder.build());
      case RATE_LIMIT:
        blockingDetailsBuilder.setCategory(BlockingCategory.BLOCKING_CATEGORY_RATE_LIMIT);
        blockingDetailsBuilder.setActorDetails(
            ActorDetails.newBuilder()
                .addAllIpAddresses(blockingPolicyData.getIpAddresses())
                .setUserId(blockingPolicyData.getUserId()));
        actorDetails = blockingDetailsBuilder.build();
        // for backward compatibility for agent version 1.24
        blockingDetailsBuilder
            .clearActorDetails()
            .setIpDetails(
                IpDetails.newBuilder().addAllIpAddresses(blockingPolicyData.getIpAddresses()));
        return List.of(actorDetails, blockingDetailsBuilder.build());
      case MODSECURITY:
        blockingDetailsBuilder.setCategory(BlockingCategory.BLOCKING_CATEGORY_MODSECURITY);
        blockingDetailsBuilder.setModsecDetails(
            ModsecDetails.newBuilder().setRuleId(blockingPolicyData.getRuleId()));
        return List.of(blockingDetailsBuilder.build());
      case CUSTOM_IP_RULE:
        blockingDetailsBuilder.setCategory(BlockingCategory.BLOCKING_CATEGORY_CUSTOM_IP_RULE);
        blockingDetailsBuilder.setIpDetails(
            IpDetails.newBuilder()
                .addAllIpAddresses(blockingPolicyData.getIpAddresses())
                .addAllIpRanges(blockingPolicyData.getIpRanges()));
        return List.of(blockingDetailsBuilder.build());
      case CUSTOM_REGION_RULE:
        blockingDetailsBuilder.setCategory(BlockingCategory.BLOCKING_CATEGORY_CUSTOM_REGION_RULE);
        blockingDetailsBuilder.setRegionDetails(
            RegionDetails.newBuilder().addAllRegions(blockingPolicyData.getRegions()));
        return List.of(blockingDetailsBuilder.build());
      case CUSTOM_SIGNATURE_RULE:
        blockingDetailsBuilder.setCategory(
            BlockingCategory.BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE);
        blockingDetailsBuilder.setCustomSignatureDetails(
            CustomSignatureDetails.newBuilder().setRuleId(blockingPolicyData.getRuleId()));
        return List.of(blockingDetailsBuilder.build());
      case IP_TYPE_RULE:
        blockingDetailsBuilder.setCategory(
            BlockingCategory.BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE);
        blockingDetailsBuilder.setIpTypeDetails(
            IpTypeDetails.newBuilder()
                .addAllIpTypes(
                    blockingPolicyData.getIpTypes().stream()
                        .map(IpTypeConverter::getIpType)
                        .collect(Collectors.toUnmodifiableList())));
        return List.of(blockingDetailsBuilder.build());
      case DATA_EXFILTRATION:
        blockingDetailsBuilder.setCategory(BlockingCategory.BLOCKING_CATEGORY_DATA_EXFILTRATION);
        blockingDetailsBuilder.setActorDetails(
            ActorDetails.newBuilder()
                .addAllIpAddresses(blockingPolicyData.getIpAddresses())
                .setUserId(blockingPolicyData.getUserId()));
        actorDetails = blockingDetailsBuilder.build();
        // for backward compatibility for agent version 1.24
        blockingDetailsBuilder
            .clearActorDetails()
            .setIpDetails(
                IpDetails.newBuilder().addAllIpAddresses(blockingPolicyData.getIpAddresses()));
        return List.of(actorDetails, blockingDetailsBuilder.build());
      case ENUMERATION:
        blockingDetailsBuilder.setCategory(BlockingCategory.BLOCKING_CATEGORY_ENUMERATION);
        blockingDetailsBuilder.setActorDetails(
            ActorDetails.newBuilder()
                .addAllIpAddresses(blockingPolicyData.getIpAddresses())
                .setUserId(blockingPolicyData.getUserId()));
        actorDetails = blockingDetailsBuilder.build();
        // for backward compatibility for agent version 1.24
        blockingDetailsBuilder
            .clearActorDetails()
            .setIpDetails(
                IpDetails.newBuilder().addAllIpAddresses(blockingPolicyData.getIpAddresses()));
        return List.of(actorDetails, blockingDetailsBuilder.build());
      case EMAIL_DOMAIN_RULE:
        blockingDetailsBuilder.setCategory(
            BlockingCategory.BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE);
        blockingDetailsBuilder.setActorDetails(
            ActorDetails.newBuilder()
                .addAllIpAddresses(blockingPolicyData.getIpAddresses())
                .setUserId(blockingPolicyData.getUserId()));
        actorDetails = blockingDetailsBuilder.build();
        // for backward compatibility for agent version 1.24
        blockingDetailsBuilder
            .clearActorDetails()
            .setIpDetails(
                IpDetails.newBuilder().addAllIpAddresses(blockingPolicyData.getIpAddresses()));
        return List.of(actorDetails, blockingDetailsBuilder.build());
      default:
        log.error(
            "Cannot find blocking category corresponding to - {}",
            blockingPolicyData.getCategory());
        blockingDetailsBuilder.setCategory(BlockingCategory.BLOCKING_CATEGORY_UNSPECIFIED);
        return List.of(blockingDetailsBuilder.build());
    }
  }

  private static BlockingRuleType getBlockingRuleType(RuleType ruleType) {
    switch (ruleType) {
      case ALLOW:
        return BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
      case BLOCK:
        return BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
      case BLOCK_ALL_EXCEPT:
        return BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT;
      default:
        log.error("Cannot find blocking rule type corresponding to - {}", ruleType);
        return BlockingRuleType.BLOCKING_RULE_TYPE_UNSPECIFIED;
    }
  }

  private static BlockingStatus getBlockingStatus(Status status) {
    switch (status) {
      case ALLOWED:
        return BlockingStatus.BLOCKING_STATUS_ALLOWED;
      case DENIED:
        return BlockingStatus.BLOCKING_STATUS_DENIED;
      case SUSPENDED:
        return BlockingStatus.BLOCKING_STATUS_SUSPENDED;
      case SNOOZED:
        return BlockingStatus.BLOCKING_STATUS_SNOOZED;
      default:
        log.error("Cannot find blocking status type corresponding to - {}", status);
        return BlockingStatus.BLOCKING_STATUS_UNSPECIFIED;
    }
  }
}
