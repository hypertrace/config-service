package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location;

import org.junit.jupiter.api.Test;

class JsonRequestBodyLocationTranslatorTest {
  private final JsonRequestBodyLocationTranslator locationTranslator =
      new JsonRequestBodyLocationTranslator();

  @Test
  void addAttributeAction() {
    JwtLocationTestUtils.assertAttributeRuleWithActions(
        locationTranslator,
        "jwt_extraction/add_attribute_action/json_request_body/input_rule.json",
        "jwt_extraction/add_attribute_action/json_request_body/jwt_extraction_actions.json");
  }
}
