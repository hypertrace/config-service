package ai.traceable.blocking.config.service.common.blockingpolicy.data;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public final class BlockingPolicyData {
  Category category;
  RuleType ruleType;
  String info;
  long timestamp;
  Status status;
  BlockingPolicyDataBucket bucket;
  BlockingDetails blockingDetails;

  public enum Category {
    THREAT_ACTOR,
    RATE_LIMIT,
    MODSECURITY,
    CUSTOM_IP_RULE,
    CUSTOM_REGION_RULE,
    CUSTOM_SIGNATURE_RULE,
    IP_TYPE_RULE,
    EMAIL_DOMAIN_RULE,
    DATA_EXFILTRATION,
    ENUMERATION,
    TRANSACTION_BASED_DLP
  }

  public enum RuleType {
    ALLOW,
    BLOCK,
    BLOCK_ALL_EXCEPT
  }

  public enum Status {
    ALLOWED,
    DENIED,
    SUSPENDED,
    SNOOZED
  }
}
