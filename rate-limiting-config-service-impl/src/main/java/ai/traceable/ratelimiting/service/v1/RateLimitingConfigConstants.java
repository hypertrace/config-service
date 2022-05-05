package ai.traceable.ratelimiting.service.v1;

public interface RateLimitingConfigConstants {
  // For Rule to RateLimitedEntity association record.
  String RULE_RATE_LIMITED_ENTITY_ASSOCIATION_RESOURCE_NAME = "rateLimitEntityAssociationConfig";

  // For the Rate limiting rule config records.
  String RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME = " rateLimitRuleConfig";

  String RATE_LIMITING_NAMESPACE = "rateLimiting";
}
