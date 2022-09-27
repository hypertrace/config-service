package ai.traceable.external.agent.attribute.config.service.translator;

import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BasicAuthRuleTranslatorTest {

  private final BasicAuthRuleTranslator basicAuthRuleTranslator =
      new BasicAuthRuleTranslator(new AttributeKeysExtractor(), new AttributeRuleBuilder());

  @Test
  void translateRuleForUserId() throws IOException {
    List<AttributeRule> translatedRules =
        basicAuthRuleTranslator
            .translateRuleForUserId(TestUtils.getUserAttributionRule("basic_auth/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("basic_auth/user_id_rules.json"), translatedRules);
  }

  @Test
  void translateRuleForAuthType() throws IOException {
    List<AttributeRule> translatedRules =
        basicAuthRuleTranslator
            .translateRuleForAuthType(
                TestUtils.getUserAttributionRule("basic_auth/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("basic_auth/auth_type_rules.json"), translatedRules);
  }
}
