package ai.traceable.customsignature.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import java.util.List;
import org.junit.jupiter.api.Test;

class CustomSignatureRulesEdgeDecisionFilterTest {

  @Test
  void testIsExpiredRule_NoExpiryDetails_ReturnsFalse() {
    CustomSignatureRule rule = CustomSignatureRule.newBuilder().setId("r1").build();
    assertFalse(CustomSignatureRulesEdgeDecisionFilter.isExpiredRule(rule));
  }

  @Test
  void testIsExpiredRule_FutureTimestamp_ReturnsFalse() {
    CustomSignatureRule rule =
        CustomSignatureRule.newBuilder()
            .setId("r1")
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder()
                    .setExpiryTimestampMillis(System.currentTimeMillis() + 600_000)
                    .build())
            .build();
    assertFalse(CustomSignatureRulesEdgeDecisionFilter.isExpiredRule(rule));
  }

  @Test
  void testIsExpiredRule_PastTimestamp_ReturnsTrue() {
    CustomSignatureRule rule =
        CustomSignatureRule.newBuilder()
            .setId("r1")
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder()
                    .setExpiryTimestampMillis(System.currentTimeMillis() - 600_000)
                    .build())
            .build();
    assertTrue(CustomSignatureRulesEdgeDecisionFilter.isExpiredRule(rule));
  }

  @Test
  void testGetConvertibleAndActiveRules_FiltersExpiredAndNonEdgeRules() {
    CustomSignatureRule activeEdgeRule =
        CustomSignatureRule.newBuilder()
            .setId("active-edge")
            .setEffect(
                RuleEffect.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE))
            .build();

    CustomSignatureRule expiredEdgeRule =
        CustomSignatureRule.newBuilder()
            .setId("expired-edge")
            .setEffect(
                RuleEffect.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE))
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder()
                    .setExpiryTimestampMillis(System.currentTimeMillis() - 600_000))
            .build();

    CustomSignatureRule platformRule =
        CustomSignatureRule.newBuilder()
            .setId("platform")
            .setEffect(
                RuleEffect.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
            .build();

    CustomSignatureRule futureExpiryEdgeRule =
        CustomSignatureRule.newBuilder()
            .setId("future-edge")
            .setEffect(
                RuleEffect.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE))
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder()
                    .setExpiryTimestampMillis(System.currentTimeMillis() + 600_000))
            .build();

    List<CustomSignatureRule> result =
        CustomSignatureRulesEdgeDecisionFilter.getConvertibleAndActiveRules(
            List.of(activeEdgeRule, expiredEdgeRule, platformRule, futureExpiryEdgeRule));

    assertEquals(2, result.size());
    assertTrue(result.stream().anyMatch(r -> r.getId().equals("active-edge")));
    assertTrue(result.stream().anyMatch(r -> r.getId().equals("future-edge")));
    assertFalse(result.stream().anyMatch(r -> r.getId().equals("expired-edge")));
    assertFalse(result.stream().anyMatch(r -> r.getId().equals("platform")));
  }
}
