package ai.traceable.ratelimiting.config.service.v2;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceConfig;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesValidator;
import com.typesafe.config.Config;
import java.util.List;
import java.util.UUID;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class RateLimitingConfigServiceConfigTest {
  private final RateLimitingRulesValidator rulesValidator = new RateLimitingRulesValidator();

  @Test
  void testConfig() {
    RateLimitingConfigServiceConfig rateLimitingConfigServiceConfig =
        new RateLimitingConfigServiceConfig(mock(Config.class));
    List<RateLimitingRule> rateLimitingRules =
        rateLimitingConfigServiceConfig.getDefaultRateLimitingRules();
    assertDefaultRateLimitingRules(rateLimitingRules);
  }

  private void assertDefaultRateLimitingRules(List<RateLimitingRule> rateLimitingRules) {

    int rateLimitingRulesCount = rateLimitingRules.size();
    assertEquals(6, rateLimitingRulesCount);

    assertEquals(
        rateLimitingRulesCount,
        rateLimitingRules.stream()
            .map(RateLimitingRule::getId)
            .filter(id -> !id.isBlank())
            .distinct()
            .count());
    assertEquals(
        rateLimitingRulesCount,
        rateLimitingRules.stream()
            .map(rule -> rule.getData().getName())
            .filter(name -> !name.isBlank())
            .distinct()
            .count());
    rateLimitingRules.forEach(
        rateLimitingRule -> assertFalse(rateLimitingRule.getData().getEnabled()));
    assertTrue(
        rateLimitingRules.stream()
            .allMatch(
                rule ->
                    rule.getData()
                        .getRuleStatus()
                        .getRuleCreationSource()
                        .equals(RuleStatus.RuleSource.RULE_SOURCE_DEFAULT)));
    // rule ids conform to UUID
    assertDoesNotThrow(() -> rateLimitingRules.forEach(rule -> UUID.fromString(rule.getId())));
    rateLimitingRules.forEach(
        rule -> {
          RuleStatus ruleStatus =
              rule.getData().getRuleStatus().toBuilder().clearRuleCreationSource().build();
          RateLimitingRuleData ruleData =
              rule.getData().toBuilder().setRuleStatus(ruleStatus).build();
          UpdateRateLimitingRuleRequest request =
              UpdateRateLimitingRuleRequest.newBuilder()
                  .setRuleId(rule.getId())
                  .setData(ruleData)
                  .build();
          assertDoesNotThrow(
              () ->
                  rulesValidator.validateOrThrow(
                      RequestContext.forTenantId("default tenant"), request, List.of()));
        });
  }
}
