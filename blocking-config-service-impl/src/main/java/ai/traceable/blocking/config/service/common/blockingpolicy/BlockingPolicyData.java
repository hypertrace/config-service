package ai.traceable.blocking.config.service.common.blockingpolicy;

import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import java.util.List;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class BlockingPolicyData {
  Category category;
  RuleType ruleType;
  String info;
  long timestamp;
  Status status;
  String ruleId;
  String userId;
  List<String> ipAddresses;
  List<String> ipRanges;
  List<String> regions;
  List<IpLocationType> ipTypes;

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
    ENUMERATION
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
