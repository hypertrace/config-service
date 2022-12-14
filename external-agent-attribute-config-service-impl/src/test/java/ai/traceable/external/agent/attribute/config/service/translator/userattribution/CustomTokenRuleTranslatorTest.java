package ai.traceable.external.agent.attribute.config.service.translator.userattribution;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.translator.TestUtils;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CustomTokenRuleTranslatorTest {

  private final CustomTokenRuleTranslator customTokenRuleTranslator =
      new CustomTokenRuleTranslator(new AttributeKeysExtractor(), new AttributeRuleBuilder());

  @Test
  void translateRequestHeaderRuleForAuthType() throws IOException {
    List<AttributeRule> translatedRules =
        customTokenRuleTranslator
            .translateRuleForAuthType(
                TestUtils.getUserAttributionRule("custom_token/request_header/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("custom_token/request_header/auth_type_rules.json"),
        translatedRules);
  }

  @Test
  void translateRequestCookieRuleForAuthType() throws IOException {
    List<AttributeRule> translatedRules =
        customTokenRuleTranslator
            .translateRuleForAuthType(
                TestUtils.getUserAttributionRule("custom_token/request_cookie/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("custom_token/request_cookie/auth_type_rules.json"),
        translatedRules);
  }

  @Test
  void translateRequestBodyRuleForAuthType() throws IOException {
    List<AttributeRule> translatedRules =
        customTokenRuleTranslator
            .translateRuleForAuthType(
                TestUtils.getUserAttributionRule("custom_token/request_body/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("custom_token/request_body/auth_type_rules.json"),
        translatedRules);
  }
}
