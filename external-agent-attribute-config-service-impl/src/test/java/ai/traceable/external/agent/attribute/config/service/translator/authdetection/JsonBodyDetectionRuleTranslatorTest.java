package ai.traceable.external.agent.attribute.config.service.translator.authdetection;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.translator.TestUtils;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JsonBodyDetectionRuleTranslatorTest {
  private final JsonBodyDetectionRuleTranslator jsonBodyDetectionRuleTranslator =
      new JsonBodyDetectionRuleTranslator(
          new AttributeRuleBuilder(),
          new PredicateBuilder(
              () -> {
                throw new UnsupportedOperationException("Not expected to be used in test");
              }));

  @Test
  void testTranslate() {
    List<AttributeRule> translatedRules =
        jsonBodyDetectionRuleTranslator
            .translateRuleForAuthType(
                TestUtils.getAuthDetectionRule("authdetection/json_body/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("authdetection/json_body/auth_type_rules.json"),
        translatedRules);
  }
}
