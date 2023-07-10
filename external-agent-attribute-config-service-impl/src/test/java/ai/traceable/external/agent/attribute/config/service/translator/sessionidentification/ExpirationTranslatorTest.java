package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import static com.google.inject.Stage.DEVELOPMENT;

import ai.traceable.external.agent.attribute.config.service.translator.TestUtils;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import com.google.inject.Guice;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ExpirationTranslatorTest {
  private final ExpirationTranslator translator =
      Guice.createInjector(DEVELOPMENT, new SessionIdentificationTranslationModule())
          .getInstance(ExpirationTranslator.class);

  @Test
  void test_jwtExpiration() {
    SessionIdentificationRule rule =
        TestUtils.getSessionIdentificationRule(
            "session_identification/expiration/partial_input_for_jwt.json");
    AttributeRule attributeRule =
        translator.translateExpiration(
            rule.getTokenRules(0).getResponseSessionTokenDetails(),
            0,
            rule.getId(),
            rule.getTokenRules(0)
                .getTokenValueRule()
                .getTokenValueProjection()
                .getAttributeProjection()
                .getAttributeKeyMatchCondition());
    AttributeRule expectedAttributeRule =
        TestUtils.getExpectedAttributeRule(
            "session_identification/expiration/partial_output_for_jwt.json");
    Assertions.assertEquals(expectedAttributeRule, attributeRule);
  }

  @Test
  void test_attributeExpiration() {
    SessionIdentificationRule rule =
        TestUtils.getSessionIdentificationRule(
            "session_identification/expiration/partial_input_for_attribute.json");
    AttributeRule attributeRule =
        translator.translateExpiration(
            rule.getTokenRules(0).getResponseSessionTokenDetails(),
            0,
            rule.getId(),
            rule.getTokenRules(0)
                .getTokenValueRule()
                .getTokenValueProjection()
                .getAttributeProjection()
                .getAttributeKeyMatchCondition());
    AttributeRule expectedAttributeRule =
        TestUtils.getExpectedAttributeRule(
            "session_identification/expiration/partial_output_for_attribute.json");
    Assertions.assertEquals(expectedAttributeRule, attributeRule);
  }
}
