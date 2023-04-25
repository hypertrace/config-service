package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location;

import org.junit.jupiter.api.Test;

class RequestHeaderLocationTranslatorTest {
  private final RequestHeaderLocationTranslator locationTranslator =
      new RequestHeaderLocationTranslator();

  @Test
  void addAttributeAction() {
    JwtLocationTestUtils.assertAttributeRuleWithActions(
        locationTranslator,
        "jwt_extraction/add_attribute_action/request_header/input_rule.json",
        "jwt_extraction/add_attribute_action/request_header/jwt_extraction_actions.json");
  }
}
