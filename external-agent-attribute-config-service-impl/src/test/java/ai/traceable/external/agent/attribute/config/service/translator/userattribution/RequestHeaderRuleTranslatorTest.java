package ai.traceable.external.agent.attribute.config.service.translator.userattribution;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.translator.TestUtils;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.HeaderLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RequestHeaderUserAttributionRuleData;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RequestHeaderRuleTranslatorTest {

  private final RequestHeaderRuleTranslator requestHeaderRuleTranslator =
      new RequestHeaderRuleTranslator(new AttributeKeysExtractor(), new AttributeRuleBuilder());

  @Test
  void translateRuleForUserId() throws IOException {
    List<AttributeRule> translatedRules =
        requestHeaderRuleTranslator
            .translateRuleForUserId(
                TestUtils.getUserAttributionRule("request_header/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("request_header/user_id_rules.json"), translatedRules);
  }

  @Test
  void translateRuleForUserRole() throws IOException {
    List<AttributeRule> translatedRules =
        requestHeaderRuleTranslator
            .translateRuleForUserRole(
                TestUtils.getUserAttributionRule("request_header/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("request_header/user_role_rules.json"),
        translatedRules);
  }

  @Test
  void translateRuleForAuthType() throws IOException {
    List<AttributeRule> translatedRules =
        requestHeaderRuleTranslator
            .translateRuleForAuthType(
                TestUtils.getUserAttributionRule("request_header/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("request_header/auth_type_rules.json"),
        translatedRules);
  }

  @Test
  void translateRuleWithMissingUserRole() {
    List<AttributeRule> translatedRules =
        requestHeaderRuleTranslator
            .translateRuleForUserRole(getRequestHeaderRuleWithUserIdOnly())
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(Collections.emptyList(), translatedRules);
  }

  @Test
  void translateJwtRuleWithMissingAuthType() {
    List<AttributeRule> translatedRules =
        requestHeaderRuleTranslator
            .translateRuleForAuthType(getRequestHeaderRuleWithUserIdOnly())
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(Collections.emptyList(), translatedRules);
  }

  private UserAttributionRule getRequestHeaderRuleWithUserIdOnly() {
    return UserAttributionRule.newBuilder()
        .setId("request-header-rule-id")
        .setData(
            UserAttributionRuleData.newBuilder()
                .setRequestHeaderData(
                    RequestHeaderUserAttributionRuleData.newBuilder()
                        .setUserIdLocation(HeaderLocation.newBuilder().setHeaderName("id-hdr"))))
        .build();
  }
}
