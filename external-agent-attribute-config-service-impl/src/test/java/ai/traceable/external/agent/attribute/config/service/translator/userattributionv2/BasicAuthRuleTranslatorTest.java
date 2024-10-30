package ai.traceable.external.agent.attribute.config.service.translator.userattributionv2;

import static com.google.inject.Stage.DEVELOPMENT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.external.agent.attribute.config.service.translator.TestUtils;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import com.google.inject.Guice;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BasicAuthRuleTranslatorTest {

  private final UserAttributionRuleV2Translator userAttributionRuleV2Translator =
      Guice.createInjector(DEVELOPMENT, new UserAttributionRuleV2TranslationModule())
          .getInstance(UserAttributionRuleV2Translator.class);

  @Test
  void translateRuleForUserId() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForUserId(
                TestUtils.getUserAttributionRuleV2("basic_auth_v2/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules("basic_auth_v2/user_id_rules.json"), translatedRules);
  }

  @Test
  void translateRuleForAuthType() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForAuthType(
                TestUtils.getUserAttributionRuleV2("basic_auth_v2/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(Collections.emptyList(), translatedRules);
  }
}
