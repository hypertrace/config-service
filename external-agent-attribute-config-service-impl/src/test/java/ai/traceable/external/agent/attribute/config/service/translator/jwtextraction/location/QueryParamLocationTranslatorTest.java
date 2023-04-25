package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location;

import org.junit.jupiter.api.Test;

class QueryParamLocationTranslatorTest {
  private final QueryParamLocationTranslator locationTranslator =
      new QueryParamLocationTranslator();

  @Test
  void addAttributeAction() {
    JwtLocationTestUtils.assertAttributeRuleWithActions(
        locationTranslator,
        "jwt_extraction/add_attribute_action/query_param/input_rule.json",
        "jwt_extraction/add_attribute_action/query_param/jwt_extraction_actions.json");
  }
}
