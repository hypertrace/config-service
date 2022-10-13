package ai.traceable.external.agent.attribute.config.service.translator;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ExternalAgentAttributeRuleTranslatorTest {

  private final AttributeKeysExtractor attributeKeysExtractor = new AttributeKeysExtractor();
  private final AttributeRuleBuilder attributeRuleBuilder = new AttributeRuleBuilder();
  private final ExternalAgentAttributeRuleTranslator translator =
      new ExternalAgentAttributeRuleTranslator(
          Set.of(
              new BasicAuthRuleTranslator(attributeKeysExtractor, attributeRuleBuilder),
              new CustomTokenRuleTranslator(attributeKeysExtractor, attributeRuleBuilder),
              new JwtRuleTranslator(attributeKeysExtractor, attributeRuleBuilder),
              new RequestHeaderRuleTranslator(attributeKeysExtractor, attributeRuleBuilder),
              new ResponseBodyRuleTranslator(attributeRuleBuilder)),
          attributeRuleBuilder,
          new UrlScopeTranslator(attributeRuleBuilder));

  @Test
  void translateRules() throws IOException {
    List<AttributeRule> translatedRules =
        translator.translateRules(
            List.of(
                TestUtils.getUserAttributionRule("basic_auth/input_rule.json"),
                TestUtils.getUserAttributionRule("custom_token/request_body/input_rule.json"),
                TestUtils.getUserAttributionRule("jwt/header/input_rule.json"),
                TestUtils.getUserAttributionRule("request_header/input_rule.json"),
                TestUtils.getUserAttributionRule("response_body/input_rule.json")));

    assertEquals(
        TestUtils.getExpectedAttributeRules("agent_attribute_rules.json"), translatedRules);
  }

  @Test
  void translateNoRule() {
    assertEquals(List.of(), translator.translateRules(emptyList()));
  }
}
