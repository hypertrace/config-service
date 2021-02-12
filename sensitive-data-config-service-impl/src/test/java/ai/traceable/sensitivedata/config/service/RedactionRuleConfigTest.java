package ai.traceable.sensitivedata.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.sensitivedata.config.service.v1.MatchType;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import org.junit.jupiter.api.Test;

class RedactionRuleConfigTest {

  @Test
  void conversionToAndFromValue() {
    RedactionRuleConfig redactionRuleConfig = new RedactionRuleConfig(getRedactionRule());
    assertEquals(redactionRuleConfig, RedactionRuleConfig.fromValue(redactionRuleConfig.toValue()));
  }

  private RedactionRule getRedactionRule() {
    return RedactionRule.newBuilder()
        .setId("id-1")
        .setName("rule-1")
        .setDescription("sample rule")
        .setCategory("auth")
        .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_REDACT)
        .setMatchType(MatchType.MATCH_TYPE_KEY)
        .setRegex("^password")
        .build();
  }
}
