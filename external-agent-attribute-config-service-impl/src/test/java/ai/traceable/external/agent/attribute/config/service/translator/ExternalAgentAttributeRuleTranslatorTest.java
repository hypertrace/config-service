package ai.traceable.external.agent.attribute.config.service.translator;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.external.agent.attribute.config.service.ExternalAgentAttributeConfigServiceConfig;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRules;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ExternalAgentAttributeRuleTranslatorTest {
  private ExternalAgentAttributeConfigServiceConfig mockConfig =
      mock(ExternalAgentAttributeConfigServiceConfig.class);

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
          new UrlScopeTranslator(attributeRuleBuilder),
          mockConfig);

  @Test
  void translateRules() throws IOException {
    AgentAttributeRules translatedRules =
        translator.translateRules(
            List.of(
                TestUtils.getUserAttributionRule("basic_auth/input_rule.json"),
                TestUtils.getUserAttributionRule("custom_token/request_body/input_rule.json"),
                TestUtils.getUserAttributionRule("jwt/header/input_rule.json"),
                TestUtils.getUserAttributionRule("request_header/input_rule.json"),
                TestUtils.getUserAttributionRule("response_body/input_rule.json")));
    assertEquals(
        TestUtils.getExpectedAgentAttributeRules("agent_attribute_rules.json"), translatedRules);
  }

  @Test
  void translateNoRule() {
    assertEquals(AgentAttributeRules.getDefaultInstance(), translator.translateRules(emptyList()));
  }

  @Test
  void translateSystemRules() throws IOException {
    final AgentAttributeRules rules =
        TestUtils.getExpectedAgentAttributeRules("system_rules/input_system_rules.json");
    when(mockConfig.getSystemAuthTypeRules())
        .thenReturn(
            rules
                .getAgentAttributeRule(0)
                .getRootRule()
                .getProjector()
                .getEachMatchingProjector()
                .getAttributeRules(0)
                .getProjector()
                .getEachMatchingProjector()
                .getAttributeRulesList());

    final AgentAttributeRules translatedRules = translator.translateRules(emptyList());
    assertEquals(rules, translatedRules);
  }
}
