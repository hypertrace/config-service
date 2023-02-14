package ai.traceable.ratelimiting.config.service.v2;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceConfig;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesValidator;
import com.typesafe.config.Config;
import java.util.List;
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
    assertDoesNotThrow(
        () ->
            rateLimitingRules.forEach(
                rule ->
                    rulesValidator.validateOrThrow(
                        RequestContext.forTenantId("default tenant"),
                        UpdateRateLimitingRuleRequest.newBuilder()
                            .setRuleId(rule.getId())
                            .setData(rule.getData())
                            .build(),
                        List.of())));
  }
}
