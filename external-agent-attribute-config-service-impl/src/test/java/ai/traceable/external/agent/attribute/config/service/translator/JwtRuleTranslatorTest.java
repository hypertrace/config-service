package ai.traceable.external.agent.attribute.config.service.translator;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.EncodedLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.JwtUserAttributionRuleData;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JwtRuleTranslatorTest {

  private final JwtRuleTranslator jwtRuleTranslator =
      new JwtRuleTranslator(new AttributeKeysExtractor(), new AttributeRuleBuilder());

  @Test
  void translateJwtHeaderRuleForUserId() throws IOException {
    List<AttributeRule> translatedRules =
        jwtRuleTranslator
            .translateRuleForUserId(TestUtils.getUserAttributionRule("jwt/header/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("jwt/header/user_id_rules.json"), translatedRules);
  }

  @Test
  void translateJwtHeaderRuleForUserRole() throws IOException {
    List<AttributeRule> translatedRules =
        jwtRuleTranslator
            .translateRuleForUserRole(
                TestUtils.getUserAttributionRule("jwt/header/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("jwt/header/user_role_rules.json"), translatedRules);
  }

  @Test
  void translateJwtHeaderRuleForAuthType() throws IOException {
    List<AttributeRule> translatedRules =
        jwtRuleTranslator
            .translateRuleForAuthType(
                TestUtils.getUserAttributionRule("jwt/header/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("jwt/header/auth_type_rules.json"), translatedRules);
  }

  @Test
  void translateJwtCookieRuleForUserId() throws IOException {
    List<AttributeRule> translatedRules =
        jwtRuleTranslator
            .translateRuleForUserId(TestUtils.getUserAttributionRule("jwt/cookie/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("jwt/cookie/user_id_rules.json"), translatedRules);
  }

  @Test
  void translateJwtCookieRuleForUserRole() throws IOException {
    List<AttributeRule> translatedRules =
        jwtRuleTranslator
            .translateRuleForUserRole(
                TestUtils.getUserAttributionRule("jwt/cookie/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("jwt/cookie/user_role_rules.json"), translatedRules);
  }

  @Test
  void translateJwtCookieRuleForAuthType() throws IOException {
    List<AttributeRule> translatedRules =
        jwtRuleTranslator
            .translateRuleForAuthType(
                TestUtils.getUserAttributionRule("jwt/cookie/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("jwt/cookie/auth_type_rules.json"), translatedRules);
  }

  @Test
  void translateJwtRuleWithMissingUserRole() {
    List<AttributeRule> translatedRules =
        jwtRuleTranslator
            .translateRuleForUserRole(getJwtHeaderRuleWithUserIdOnly())
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(Collections.emptyList(), translatedRules);
  }

  @Test
  void translateJwtRuleWithMissingAuthType() {
    List<AttributeRule> translatedRules =
        jwtRuleTranslator
            .translateRuleForAuthType(getJwtHeaderRuleWithUserIdOnly())
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(Collections.emptyList(), translatedRules);
  }

  private UserAttributionRule getJwtHeaderRuleWithUserIdOnly() {
    return UserAttributionRule.newBuilder()
        .setId("jwt-header-rule-id")
        .setData(
            UserAttributionRuleData.newBuilder()
                .setJwtData(
                    JwtUserAttributionRuleData.newBuilder()
                        .setUserIdClaim("data-claim")
                        .setUserIdLocation(EncodedLocation.newBuilder().setJsonPath("$.id"))))
        .build();
  }
}
