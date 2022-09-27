package ai.traceable.external.agent.attribute.config.service.translator;

import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.EncodedLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.ResponseBodyUserAttributionRuleData;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ResponseBodyRuleTranslatorTest {

  private final ResponseBodyRuleTranslator responseBodyRuleTranslator =
      new ResponseBodyRuleTranslator(new AttributeRuleBuilder());

  @Test
  void translateRuleForUserId() throws IOException {
    List<AttributeRule> translatedRules =
        responseBodyRuleTranslator
            .translateRuleForUserId(
                TestUtils.getUserAttributionRule("response_body/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("response_body/user_id_rules.json"), translatedRules);
  }

  @Test
  void translateRuleForUserRole() throws IOException {
    List<AttributeRule> translatedRules =
        responseBodyRuleTranslator
            .translateRuleForUserRole(
                TestUtils.getUserAttributionRule("response_body/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("response_body/user_role_rules.json"), translatedRules);
  }

  @Test
  void translateRuleForAuthType() throws IOException {
    List<AttributeRule> translatedRules =
        responseBodyRuleTranslator
            .translateRuleForAuthType(
                TestUtils.getUserAttributionRule("response_body/input_rule.json"))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules("response_body/auth_type_rules.json"), translatedRules);
  }

  @Test
  void translateRuleWithMissingUserRole() {
    List<AttributeRule> translatedRules =
        responseBodyRuleTranslator
            .translateRuleForUserRole(getResponseBodyRuleWithUserIdOnly())
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(Collections.emptyList(), translatedRules);
  }

  @Test
  void translateJwtRuleWithMissingAuthType() {
    List<AttributeRule> translatedRules =
        responseBodyRuleTranslator
            .translateRuleForAuthType(getResponseBodyRuleWithUserIdOnly())
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(Collections.emptyList(), translatedRules);
  }

  private UserAttributionRule getResponseBodyRuleWithUserIdOnly() {
    return UserAttributionRule.newBuilder()
        .setId("response-body-rule-id")
        .setData(
            UserAttributionRuleData.newBuilder()
                .setResponseBodyData(
                    ResponseBodyUserAttributionRuleData.newBuilder()
                        .setUserIdLocation(EncodedLocation.newBuilder().setJsonPath("$.id"))))
        .build();
  }
}
