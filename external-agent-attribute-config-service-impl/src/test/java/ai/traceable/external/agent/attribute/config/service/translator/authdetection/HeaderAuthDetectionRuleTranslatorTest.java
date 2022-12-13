package ai.traceable.external.agent.attribute.config.service.translator.authdetection;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.translator.TestUtils;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class HeaderAuthDetectionRuleTranslatorTest {
  private final HeaderAuthDetectionRuleTranslator headerAuthDetectionRuleTranslator =
      new HeaderAuthDetectionRuleTranslator(
          new AttributeRuleBuilder(),
          new PredicateBuilder(
              () -> {
                throw new UnsupportedOperationException("Not expected to be used in test");
              }));

  @Test
  void testTranslate() {
    List<AttributeRule> translatedRules =
        headerAuthDetectionRuleTranslator
            .translateRuleForAuthType(
                TestUtils.getAuthDetectionRule("authdetection/header/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("authdetection/header/auth_type_rules.json"),
        translatedRules);
  }
}
