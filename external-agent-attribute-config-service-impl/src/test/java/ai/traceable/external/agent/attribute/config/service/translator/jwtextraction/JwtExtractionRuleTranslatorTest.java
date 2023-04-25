package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction;

import ai.traceable.external.agent.attribute.config.service.translator.TestUtils;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import com.google.inject.Guice;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JwtExtractionRuleTranslatorTest {

  @Test
  void testJwtExtractionTranslation() {
    JwtExtractionRuleTranslator translator =
        Guice.createInjector(new JwtExtractionTranslationModule())
            .getInstance(JwtExtractionRuleTranslator.class);
    JwtExtractionRule inputRule = TestUtils.getJwtExtractionRule("jwt_extraction/input_rule.json");
    List<AttributeRule> translatedRules =
        translator
            .translateJwtExtractionRules(List.of(inputRule))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("jwt_extraction/output_agent_attribute_rules.json"),
        translatedRules);
  }
}
