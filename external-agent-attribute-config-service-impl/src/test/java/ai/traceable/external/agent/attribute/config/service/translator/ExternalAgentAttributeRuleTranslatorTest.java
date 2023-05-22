package ai.traceable.external.agent.attribute.config.service.translator;

import static com.google.inject.Stage.DEVELOPMENT;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.external.agent.attribute.config.service.translator.authdetection.AuthDetectionRuleTranslationModule;
import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.JwtExtractionTranslationModule;
import ai.traceable.external.agent.attribute.config.service.translator.userattribution.UserAttributionRuleTranslationModule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import com.google.inject.Guice;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExternalAgentAttributeRuleTranslatorTest {

  private final ExternalAgentAttributeRuleTranslator translator =
      Guice.createInjector(
              DEVELOPMENT,
              new UserAttributionRuleTranslationModule(),
              new AuthDetectionRuleTranslationModule(),
              new JwtExtractionTranslationModule())
          .getInstance(ExternalAgentAttributeRuleTranslator.class);

  @Test
  void translateRules() throws IOException {
    List<AttributeRule> attributeRules =
        translator.translateRules(
            List.of(
                TestUtils.getUserAttributionRule("basic_auth/input_rule.json"),
                TestUtils.getUserAttributionRule("custom_token/request_body/input_rule.json"),
                TestUtils.getUserAttributionRule("jwt/header/input_rule.json"),
                TestUtils.getUserAttributionRule("request_header/input_rule.json"),
                TestUtils.getUserAttributionRule("response_body/input_rule.json")),
            List.of(
                TestUtils.getAuthDetectionRule("authdetection/composite/input_rule.json"),
                TestUtils.getAuthDetectionRule("authdetection/header/input_rule.json"),
                TestUtils.getAuthDetectionRule("authdetection/json_body/input_rule.json")),
            emptyList(),
            List.of(
                TestUtils.getServiceNamingRule("servicenaming/single_condition/input_rule.json"),
                TestUtils.getServiceNamingRule("servicenaming/disabled_rule/input_rule.json"),
                TestUtils.getServiceNamingRule(
                    "servicenaming/multiple_condition/input_rule.json")));
    assertEquals(TestUtils.getExpectedAttributeRules("agent_attribute_rules.json"), attributeRules);
  }

  @Test
  void translateNoRule() {
    assertEquals(
        List.of(), translator.translateRules(emptyList(), emptyList(), emptyList(), emptyList()));
  }
}
