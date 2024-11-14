package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import static com.google.inject.Stage.DEVELOPMENT;

import ai.traceable.external.agent.attribute.config.service.translator.TestUtils;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import com.google.inject.Guice;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CustomAttributeTranslatorTest {
  private final CustomAttributeTranslator translator =
      Guice.createInjector(DEVELOPMENT, new SessionIdentificationTranslationModule())
          .getInstance(CustomAttributeTranslator.class);

  @Test
  void test_jwtAttr() {
    SessionIdentificationRule rule =
        TestUtils.getSessionIdentificationRule(
            "session_identification/custom_attribute/partial_input_for_jwt_attr.json");
    List<AttributeRule> attributeRules =
        translator.translateCustomAttribute(
            rule.getTokenRules(0).getResponseSessionTokenDetails(),
            0,
            rule.getId(),
            rule.getTokenRules(0)
                .getTokenValueRule()
                .getTokenValueProjection()
                .getAttributeProjection());

    AttributeRule expectedAttributeRule =
        TestUtils.getExpectedAttributeRule(
            "session_identification/custom_attribute/partial_output_for_jwt_attr.json");
    Assertions.assertEquals(expectedAttributeRule, attributeRules.get(0));
  }
}
