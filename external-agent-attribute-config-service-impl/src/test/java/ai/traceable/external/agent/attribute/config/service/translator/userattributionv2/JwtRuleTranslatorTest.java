package ai.traceable.external.agent.attribute.config.service.translator.userattributionv2;

import static com.google.inject.Stage.DEVELOPMENT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.external.agent.attribute.config.service.translator.TestUtils;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import com.google.inject.Guice;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class JwtRuleTranslatorTest {

  private final UserAttributionRuleV2Translator userAttributionRuleV2Translator =
      Guice.createInjector(DEVELOPMENT, new UserAttributionRuleV2TranslationModule())
          .getInstance(UserAttributionRuleV2Translator.class);

  @Test
  void translateJwtHeaderRuleForUserId() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForUserId(
                TestUtils.getUserAttributionRuleV2("jwt_v2/header/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules("jwt_v2/header/user_id_rules.json"), translatedRules);
  }

  @Test
  void translateJwtHeaderRuleForUserRole() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForUserRole(
                TestUtils.getUserAttributionRuleV2("jwt_v2/header/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules("jwt_v2/header/user_role_rules.json"), translatedRules);
  }

  @Test
  void translateJwtHeaderRuleForAuthType() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForAuthType(
                TestUtils.getUserAttributionRuleV2("jwt_v2/header/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules("jwt_v2/header/auth_type_rules.json"), translatedRules);
  }

  @Test
  void translateJwtCookieRuleForUserId() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForUserId(
                TestUtils.getUserAttributionRuleV2("jwt_v2/cookie/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules("jwt_v2/cookie/user_id_rules.json"), translatedRules);
  }

  @Test
  void translateJwtCookieRuleForUserRole() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForUserRole(
                TestUtils.getUserAttributionRuleV2("jwt_v2/cookie/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules("jwt_v2/cookie/user_role_rules.json"), translatedRules);
  }

  @Test
  void translateJwtCookieRuleForAuthType() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForAuthType(
                TestUtils.getUserAttributionRuleV2("jwt_v2/cookie/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules("jwt_v2/cookie/auth_type_rules.json"), translatedRules);
  }
}
