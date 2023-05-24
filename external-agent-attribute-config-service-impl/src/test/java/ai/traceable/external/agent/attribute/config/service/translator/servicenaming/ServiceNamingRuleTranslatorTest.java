package ai.traceable.external.agent.attribute.config.service.translator.servicenaming;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.translator.TestUtils;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ServiceNamingRuleTranslatorTest {

  ServiceNamingRuleTranslator translator =
      new ServiceNamingRuleTranslator(new AttributeRuleBuilder());

  @Test
  void test_translateSingleConditionRule() {
    Assertions.assertEquals(
        Optional.of(
            TestUtils.getExpectedAttributeRule(
                "servicenaming/single_condition/expected_rule.json")),
        translator.buildRule(
            List.of(
                TestUtils.getServiceNamingRule("servicenaming/single_condition/input_rule.json"))));
  }

  @Test
  void test_translateMultipleConditionRule() {
    Assertions.assertEquals(
        Optional.of(
            TestUtils.getExpectedAttributeRule(
                "servicenaming/multiple_condition/expected_rule.json")),
        translator.buildRule(
            List.of(
                TestUtils.getServiceNamingRule(
                    "servicenaming/multiple_condition/input_rule.json"))));
  }

  @Test
  void test_translateDynamicRule() {
    Assertions.assertEquals(
        Optional.of(
            TestUtils.getExpectedAttributeRule("servicenaming/dynamic_naming/expected_rule.json")),
        translator.buildRule(
            List.of(
                TestUtils.getServiceNamingRule("servicenaming/dynamic_naming/input_rule.json"))));
  }

  @Test
  void test_translateDisabledRule() {
    Assertions.assertEquals(
        Optional.empty(),
        translator.buildRule(
            List.of(
                TestUtils.getServiceNamingRule("servicenaming/disabled_rule/input_rule.json"))));
  }
}
