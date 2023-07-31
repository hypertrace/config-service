package ai.traceable.blocking.config.service.v2.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator.BlockingDetailsConverterBase;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingDetailsVisitor;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Status;
import ai.traceable.blocking.config.service.v2.BlockingCategory;
import ai.traceable.blocking.config.service.v2.BlockingDetails;
import ai.traceable.blocking.config.service.v2.BlockingDetails.Builder;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCondition;
import ai.traceable.blocking.config.service.v2.BlockingRuleType;
import ai.traceable.blocking.config.service.v2.BlockingStatus;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class BlockingDetailsConverter implements BlockingDetailsConverterBase<BlockingDetails> {
  private final BlockingDetailsVisitor<BlockingDetailsCondition>
      blockingDetailsBlockingDetailsVisitor;

  @Inject
  BlockingDetailsConverter(
      BlockingDetailsVisitor<BlockingDetailsCondition> blockingDetailsVisitor) {
    this.blockingDetailsBlockingDetailsVisitor = blockingDetailsVisitor;
  }

  @Override
  public BlockingDetails convert(BlockingPolicyData blockingPolicyData) {
    Builder blockingDetailsBuilder =
        BlockingDetails.newBuilder()
            .setBlockingRuleType(convert(blockingPolicyData.getRuleType()))
            .setInfo(blockingPolicyData.getInfo())
            .setStatus(convert(blockingPolicyData.getStatus()))
            .setCategory(convert(blockingPolicyData.getCategory()));
    if (blockingPolicyData.getTimestamp() != 0) {
      blockingDetailsBuilder.setExpirationTimestamp(blockingPolicyData.getTimestamp());
    }

    BlockingDetailsCondition blockingDetailsCondition =
        blockingPolicyData.getBlockingDetails().accept(blockingDetailsBlockingDetailsVisitor);

    return setBlockingDetailsCondition(blockingDetailsBuilder, blockingDetailsCondition);
  }

  private static BlockingRuleType convert(RuleType ruleType) {
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

  private static BlockingStatus convert(Status status) {
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

  private static BlockingCategory convert(Category category) {
    switch (category) {
      case THREAT_ACTOR:
        return BlockingCategory.BLOCKING_CATEGORY_THREAT_ACTOR;
      case RATE_LIMIT:
        return BlockingCategory.BLOCKING_CATEGORY_RATE_LIMIT;
      case MODSECURITY:
        return BlockingCategory.BLOCKING_CATEGORY_MODSECURITY;
      case CUSTOM_IP_RULE:
        return BlockingCategory.BLOCKING_CATEGORY_CUSTOM_IP_RULE;
      case CUSTOM_REGION_RULE:
        return BlockingCategory.BLOCKING_CATEGORY_CUSTOM_REGION_RULE;
      case CUSTOM_SIGNATURE_RULE:
        return BlockingCategory.BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE;
      case IP_TYPE_RULE:
      case EMAIL_DOMAIN_RULE:
        return BlockingCategory.BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE;
      case DATA_EXFILTRATION:
        return BlockingCategory.BLOCKING_CATEGORY_DATA_EXFILTRATION;
      case ENUMERATION:
        return BlockingCategory.BLOCKING_CATEGORY_ENUMERATION;
      default:
        log.error("Cannot find blocking category type corresponding to - {}", category);
        return BlockingCategory.BLOCKING_CATEGORY_UNSPECIFIED;
    }
  }

  private static BlockingDetails setBlockingDetailsCondition(
      BlockingDetails.Builder builder, BlockingDetailsCondition blockingDetailsCondition) {
    switch (blockingDetailsCondition.getDetailsCase()) {
      case IP_DETAILS:
        return builder.setIpDetails(blockingDetailsCondition.getIpDetails()).build();
      case MODSEC_DETAILS:
        return builder.setModsecDetails(blockingDetailsCondition.getModsecDetails()).build();
      case CUSTOM_SIGNATURE_DETAILS:
        return builder
            .setCustomSignatureDetails(blockingDetailsCondition.getCustomSignatureDetails())
            .build();
      case REGION_DETAILS:
        return builder.setRegionDetails(blockingDetailsCondition.getRegionDetails()).build();
      case ACTOR_DETAILS:
        return builder.setActorDetails(blockingDetailsCondition.getActorDetails()).build();
      case IP_TYPE_DETAILS:
        return builder.setIpTypeDetails(blockingDetailsCondition.getIpTypeDetails()).build();
      case DETAILS_COMBINATION:
        return builder
            .setDetailsCombination(blockingDetailsCondition.getDetailsCombination())
            .build();
      default:
        log.warn(
            "Unable to convert blockingDetailsCondition:{} to v2 blocking details",
            blockingDetailsCondition);
        return builder.build();
    }
  }
}
