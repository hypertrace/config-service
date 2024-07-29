package ai.traceable.blocking.config.service.common.blockingpolicy;

/**
 * Rules follow the precedence mentioned here <a
 * href="https://traceableai.atlassian.net/wiki/spaces/Engineering/pages/1265139838/Blocking+rules+-+evaluation+order+of+precedence">...</a>
 * Current order - "malicious-source-ip-range-exemption", "custom-ip-based-exemption",
 * "threat-actor-exemption", "email-domain-exemption", "custom-signature-exemption",
 * "custom-signature-violation", "modsec-violation", "malicious-source-ip-range-block-all-except",
 * "custom-ip-based-block-all-except", "malicious-source-ip-range-violation",
 * "custom-ip-based-violation", "threat-actor-violation", "email-domain-violation",
 * "malicious-source-ip-type-violation", "malicious-source-region-block-all-except",
 * "region-block-all-except", "malicious-source-region-violation", "region-violation",
 * "rate-limit-violation"
 */
public enum BlockingPolicyDataBucket {
  DATA_LOSS_PREVENTION_EXEMPTIONS,
  IP_RANGE_EXEMPTIONS,
  THREAT_ACTOR_BASED_IP_EXEMPTIONS,
  EMAIL_DOMAIN_BASED_EXEMPTIONS,
  CUSTOM_SIGNATURE_EXEMPTIONS,
  CUSTOM_SIGNATURE_VIOLATIONS,
  CUSTOM_SIGNATURE_ANALYTICS,
  MODSEC_VIOLATIONS,
  IP_RANGE_BLOCK_ALL_EXCEPT_VIOLATIONS,
  DATA_LOSS_PREVENTION_VIOLATIONS,
  IP_RANGE_VIOLATIONS,
  THREAT_ACTOR_BASED_IP_VIOLATIONS,
  EMAIL_DOMAIN_BASED_VIOLATIONS,
  IP_TYPE_VIOLATIONS,
  REGION_BLOCK_ALL_EXCEPT_VIOLATIONS,
  REGION_VIOLATIONS,
  RATE_LIMITING_BASED_IP_VIOLATIONS
}
