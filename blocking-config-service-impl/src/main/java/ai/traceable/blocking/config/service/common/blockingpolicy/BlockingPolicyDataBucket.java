package ai.traceable.blocking.config.service.common.blockingpolicy;

public enum BlockingPolicyDataBucket {
  IP_TYPE_VIOLATIONS,
  IP_RANGE_VIOLATIONS,
  IP_RANGE_EXEMPTIONS,
  IP_RANGE_BLOCK_ALL_EXCEPT_VIOLATIONS,
  REGION_VIOLATIONS,
  REGION_BLOCK_ALL_EXCEPT_VIOLATIONS
}
