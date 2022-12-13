package ai.traceable.external.agent.attribute.config.service.translator.authdetection;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.translator.TestUtils;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CompositeAuthDetectionRuleTranslatorTest {

  private final AttributeRuleBuilder attributeRuleBuilder = new AttributeRuleBuilder();
  private AuthDetectionRuleTranslatorLookup ruleTranslatorLookup;
  private final PredicateBuilder predicateBuilder =
      new PredicateBuilder(() -> ruleTranslatorLookup);
  private final CompositeAuthDetectionRuleTranslator compositeAuthDetectionRuleTranslator =
      new CompositeAuthDetectionRuleTranslator(attributeRuleBuilder, predicateBuilder);

  @BeforeEach
  void beforeEach() {
    this.ruleTranslatorLookup =
        new AuthDetectionRuleTranslatorLookup(
            Set.of(
                new HeaderAuthDetectionRuleTranslator(attributeRuleBuilder, predicateBuilder),
                compositeAuthDetectionRuleTranslator));
  }

  @Test
  void testTranslate() {
    List<AttributeRule> translatedRules =
        compositeAuthDetectionRuleTranslator
            .translateRuleForAuthType(
                TestUtils.getAuthDetectionRule("authdetection/composite/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("authdetection/composite/auth_type_rules.json"),
        translatedRules);
  }
}
