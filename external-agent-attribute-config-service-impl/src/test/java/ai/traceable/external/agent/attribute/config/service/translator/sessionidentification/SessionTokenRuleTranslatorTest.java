package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import static com.google.inject.Stage.DEVELOPMENT;

import ai.traceable.external.agent.attribute.config.service.translator.TestUtils;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import com.google.inject.Guice;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SessionTokenRuleTranslatorTest {
  private final SessionTokenRuleTranslator translator =
      Guice.createInjector(DEVELOPMENT, new SessionIdentificationTranslationModule())
          .getInstance(SessionTokenRuleTranslator.class);

  @Test
  void testForRequest() {
    SessionIdentificationRule rule =
        TestUtils.getSessionIdentificationRule(
            "session_identification/session_token/partial_input_for_request.json");
    AttributeRule attributeRule =
        translator.translateSessionTokenRule(rule.getTokenRules(0), 0, rule.getId());
    AttributeRule expectedAttributeRule =
        TestUtils.getExpectedAttributeRule(
            "session_identification/session_token/partial_output_for_request.json");
    Assertions.assertEquals(expectedAttributeRule, attributeRule);
  }

  @Test
  void testForResponse() {
    SessionIdentificationRule rule =
        TestUtils.getSessionIdentificationRule(
            "session_identification/session_token/partial_input_for_response.json");
    AttributeRule attributeRule =
        translator.translateSessionTokenRule(rule.getTokenRules(0), 0, rule.getId());
    AttributeRule expectedAttributeRule =
        TestUtils.getExpectedAttributeRule(
            "session_identification/session_token/partial_output_for_response.json");
    Assertions.assertEquals(expectedAttributeRule, attributeRule);
  }
}
