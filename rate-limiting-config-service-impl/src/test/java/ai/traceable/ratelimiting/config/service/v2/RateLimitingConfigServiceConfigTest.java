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
import java.util.stream.Collectors;
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
    assertEquals(13, rateLimitingRulesCount);

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
    assertTrue(
        rateLimitingRules.stream()
            .filter(
                rule -> rule.getData().getCategory().equals(Category.CATEGORY_DATA_EXFILTRATION))
            .flatMap(rule -> getLeafConditions(rule.getData().getCondition()).stream())
            .filter(LeafCondition::hasDatatypeCondition)
            .allMatch(
                leafCondition ->
                    leafCondition
                        .getDatatypeCondition()
                        .getDataLocation()
                        .equals(DataLocation.DATA_LOCATION_RESPONSE)));
    assertTrue(
        rateLimitingRules.stream()
            .filter(rule -> rule.getData().getCategory().equals(Category.CATEGORY_ENUMERATION))
            .flatMap(rule -> getLeafConditions(rule.getData().getCondition()).stream())
            .filter(LeafCondition::hasDatatypeCondition)
            .allMatch(
                leafCondition ->
                    leafCondition
                        .getDatatypeCondition()
                        .getDataLocation()
                        .equals(DataLocation.DATA_LOCATION_REQUEST)));
    // rule ids conform to UUID
    assertDoesNotThrow(() -> rateLimitingRules.forEach(rule -> UUID.fromString(rule.getId())));
    rateLimitingRules.forEach(
        rule -> {
          RateLimitingRuleData data = rule.getData();
          RuleStatus ruleStatus =
              data.getRuleStatus().toBuilder().clearRuleCreationSource().build();
          RateLimitingRuleData ruleData = data.toBuilder().setRuleStatus(ruleStatus).build();
          UpdateRateLimitingRuleRequest request =
              UpdateRateLimitingRuleRequest.newBuilder()
                  .setRuleId(rule.getId())
                  .setData(ruleData)
                  .build();
          assertFalse(data.getEnabled());
          assertDoesNotThrow(
              () ->
                  rulesValidator.validateOrThrow(
                      RequestContext.forTenantId("default tenant"), request, List.of()));
        });
  }

  private List<LeafCondition> getLeafConditions(Condition condition) {
    switch (condition.getConditionCase()) {
      case LEAF_CONDITION:
        return List.of(condition.getLeafCondition());
      case COMPOSITE_CONDITION:
        return condition.getCompositeCondition().getChildrenList().stream()
            .flatMap(condition1 -> getLeafConditions(condition1).stream())
            .collect(Collectors.toUnmodifiableList());
      default:
        return List.of();
    }
  }
}
