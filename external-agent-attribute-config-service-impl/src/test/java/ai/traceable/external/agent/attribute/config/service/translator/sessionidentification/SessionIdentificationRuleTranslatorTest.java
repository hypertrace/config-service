package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import static com.google.inject.Stage.DEVELOPMENT;

import ai.traceable.external.agent.attribute.config.service.translator.TestUtils;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import com.google.inject.Guice;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SessionIdentificationRuleTranslatorTest {
  private final SessionIdentificationRuleTranslator translator =
      Guice.createInjector(DEVELOPMENT, new SessionIdentificationTranslationModule())
          .getInstance(SessionIdentificationRuleTranslator.class);

  @Test
  void test() {
    SessionIdentificationRule rule =
        TestUtils.getSessionIdentificationRule("session_identification/complete_input_rule.json");
    List<AttributeRule> attributeRules =
        translator
            .translateSessionIdentificationRules(List.of(rule))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(1, attributeRules.size());
    AttributeRule expectedAttributeRule =
        TestUtils.getExpectedAttributeRule("session_identification/complete_output_rule.json");
    Assertions.assertEquals(expectedAttributeRule, attributeRules.get(0));
  }
}
