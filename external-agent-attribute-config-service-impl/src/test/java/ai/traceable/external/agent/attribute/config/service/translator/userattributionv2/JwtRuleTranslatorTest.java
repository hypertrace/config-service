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
                TestUtils.getUserAttributionRuleV2(
                    "user_attribution_v2/jwt/header/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules("user_attribution_v2/jwt/header/user_id_rules.json"),
        translatedRules);
  }

  @Test
  void translateJwtHeaderRuleForUserRole() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForUserRole(
                TestUtils.getUserAttributionRuleV2(
                    "user_attribution_v2/jwt/header/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules("user_attribution_v2/jwt/header/user_role_rules.json"),
        translatedRules);
  }

  @Test
  void translateJwtHeaderRuleForAuthType() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForAuthType(
                TestUtils.getUserAttributionRuleV2(
                    "user_attribution_v2/jwt/header/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules("user_attribution_v2/jwt/header/auth_type_rules.json"),
        translatedRules);
  }

  @Test
  void translateJwtHeaderContainsRuleForUserId() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForUserId(
                TestUtils.getUserAttributionRuleV2(
                    "user_attribution_v2/jwt/header_contains/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules(
            "user_attribution_v2/jwt/header_contains/user_id_rules.json"),
        translatedRules);
  }

  @Test
  void translateJwtHeaderContainsRuleForUserRole() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForUserRole(
                TestUtils.getUserAttributionRuleV2(
                    "user_attribution_v2/jwt/header_contains/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules(
            "user_attribution_v2/jwt/header_contains/user_role_rules.json"),
        translatedRules);
  }

  @Test
  void translateJwtHeaderContainsRuleForAuthType() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForAuthType(
                TestUtils.getUserAttributionRuleV2(
                    "user_attribution_v2/jwt/header_contains/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules(
            "user_attribution_v2/jwt/header_contains/auth_type_rules.json"),
        translatedRules);
  }

  @Test
  void translateJwtCookieRuleForUserId() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForUserId(
                TestUtils.getUserAttributionRuleV2(
                    "user_attribution_v2/jwt/cookie/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules("user_attribution_v2/jwt/cookie/user_id_rules.json"),
        translatedRules);
  }

  @Test
  void translateJwtCookieRuleForUserRole() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForUserRole(
                TestUtils.getUserAttributionRuleV2(
                    "user_attribution_v2/jwt/cookie/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules("user_attribution_v2/jwt/cookie/user_role_rules.json"),
        translatedRules);
  }

  @Test
  void translateJwtCookieRuleForAuthType() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForAuthType(
                TestUtils.getUserAttributionRuleV2(
                    "user_attribution_v2/jwt/cookie/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules("user_attribution_v2/jwt/cookie/auth_type_rules.json"),
        translatedRules);
  }

  @Test
  void translateJwtCookieContainsRuleForUserId() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForUserId(
                TestUtils.getUserAttributionRuleV2(
                    "user_attribution_v2/jwt/cookie_contains/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules(
            "user_attribution_v2/jwt/cookie_contains/user_id_rules.json"),
        translatedRules);
  }

  @Test
  void translateJwtCookieRuleContainsForUserRole() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForUserRole(
                TestUtils.getUserAttributionRuleV2(
                    "user_attribution_v2/jwt/cookie_contains/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules(
            "user_attribution_v2/jwt/cookie_contains/user_role_rules.json"),
        translatedRules);
  }

  @Test
  void translateJwtCookieRuleContainsForAuthType() throws IOException {
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForAuthType(
                TestUtils.getUserAttributionRuleV2(
                    "user_attribution_v2/jwt/cookie_contains/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        TestUtils.getExpectedAttributeRules(
            "user_attribution_v2/jwt/cookie_contains/auth_type_rules.json"),
        translatedRules);
  }
}
