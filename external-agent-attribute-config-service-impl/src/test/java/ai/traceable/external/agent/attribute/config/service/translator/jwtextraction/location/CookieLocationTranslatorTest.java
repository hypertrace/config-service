package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location;

import org.junit.jupiter.api.Test;

class CookieLocationTranslatorTest {
  private final CookieLocationTranslator locationTranslator = new CookieLocationTranslator();

  @Test
  void addAttributeAction() {
    JwtLocationTestUtils.assertAttributeRuleWithActions(
        locationTranslator,
        "jwt_extraction/add_attribute_action/cookie/input_rule.json",
        "jwt_extraction/add_attribute_action/cookie/jwt_extraction_actions.json");
  }
}
