package ai.traceable.external.data.classification.config.service.userattributionv2;

import static ai.traceable.external.data.classification.config.service.userattributionv2.UserAttributionConstants.CUSTOM_ATTRIBUTE_PREFIX;
import static ai.traceable.external.data.classification.config.service.userattributionv2.UserAttributionConstants.END_USER_ID_ATTRIBUTE_KEY;
import static ai.traceable.external.data.classification.config.service.userattributionv2.UserAttributionConstants.END_USER_ROLE_ATTRIBUTE_KEY;
import static ai.traceable.external.data.classification.config.service.userattributionv2.UserAttributionConstants.END_USER_SCOPE_ATTRIBUTE_KEY;
import static ai.traceable.external.data.classification.config.service.userattributionv2.UserAttributionConstants.RULE_ATTRIBUTE_KEY_SUFFIX;
import static ai.traceable.external.data.classification.config.service.v1.DataType.DataTransformation.DATA_TRANSFORMATION_OBFUSCATE;
import static ai.traceable.external.data.classification.config.service.v1.DataType.Result.RESULT_MATCH;
import static ai.traceable.external.data.classification.config.service.v1.Operator.OPERATOR_EQUALS;
import static ai.traceable.userattribution.config.service.v2.ObfuscationStrategy.OBFUSCATION_STRATEGY_HASH;
import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.external.data.classification.config.service.v1.AttributeFilter;
import ai.traceable.external.data.classification.config.service.v1.AttributePredicate;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.PathPredicate;
import ai.traceable.external.data.classification.config.service.v1.SpanFilter;
import ai.traceable.external.data.classification.config.service.v1.StringPredicate;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v2.UserAttributionTokenRule;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class UserAttributionRulesTranslatorTest {

  private static UserAttributionRulesTranslator translator;

  @BeforeAll
  static void setup() {
    translator = new UserAttributionRulesTranslator();
  }

  @Test
  void test_translateUserAttributionRules_userIdRules() {
    UserAttributionRule userIdRule1 =
        UserAttributionRule.newBuilder()
            .setId("rule-id-1")
            .setData(
                UserAttributionRuleData.newBuilder()
                    .setUserIdRule(
                        UserAttributionTokenRule.newBuilder()
                            .setTokenObfuscationStrategy(OBFUSCATION_STRATEGY_HASH)))
            .build();
    UserAttributionRule userIdRule2 =
        UserAttributionRule.newBuilder()
            .setId("rule-id-2")
            .setData(
                UserAttributionRuleData.newBuilder()
                    .setUserIdRule(UserAttributionTokenRule.getDefaultInstance()))
            .build();
    List<DataType> dataTypes =
        translator.translateUserAttributionRules(List.of(userIdRule1, userIdRule2));
    DataType expectedDataType = buildExpectedDataType(END_USER_ID_ATTRIBUTE_KEY, "rule-id-1");
    assertEquals(1, dataTypes.size());
    assertEquals(expectedDataType, dataTypes.get(0));
  }

  @Test
  void test_translateUserAttributionRules_userRoleRules() {
    UserAttributionRule userIdRule1 =
        UserAttributionRule.newBuilder()
            .setId("rule-id-1")
            .setData(
                UserAttributionRuleData.newBuilder()
                    .setUserRoleRule(
                        UserAttributionTokenRule.newBuilder()
                            .setTokenObfuscationStrategy(OBFUSCATION_STRATEGY_HASH)))
            .build();
    UserAttributionRule userIdRule2 =
        UserAttributionRule.newBuilder()
            .setId("rule-id-2")
            .setData(
                UserAttributionRuleData.newBuilder()
                    .setUserRoleRule(UserAttributionTokenRule.getDefaultInstance()))
            .build();
    List<DataType> dataTypes =
        translator.translateUserAttributionRules(List.of(userIdRule1, userIdRule2));
    DataType expectedDataType = buildExpectedDataType(END_USER_ROLE_ATTRIBUTE_KEY, "rule-id-1");
    assertEquals(1, dataTypes.size());
    assertEquals(expectedDataType, dataTypes.get(0));
  }

  @Test
  void test_translateUserAttributionRules_userScopeRules() {
    UserAttributionRule userIdRule1 =
        UserAttributionRule.newBuilder()
            .setId("rule-id-1")
            .setData(
                UserAttributionRuleData.newBuilder()
                    .setUserScopeRule(
                        UserAttributionTokenRule.newBuilder()
                            .setTokenObfuscationStrategy(OBFUSCATION_STRATEGY_HASH)))
            .build();
    UserAttributionRule userIdRule2 =
        UserAttributionRule.newBuilder()
            .setId("rule-id-2")
            .setData(
                UserAttributionRuleData.newBuilder()
                    .setUserScopeRule(UserAttributionTokenRule.getDefaultInstance()))
            .build();
    List<DataType> dataTypes =
        translator.translateUserAttributionRules(List.of(userIdRule1, userIdRule2));
    DataType expectedDataType = buildExpectedDataType(END_USER_SCOPE_ATTRIBUTE_KEY, "rule-id-1");
    assertEquals(1, dataTypes.size());
    assertEquals(expectedDataType, dataTypes.get(0));
  }

  @Test
  void test_translateUserAttributionRules_customTokenRules() {
    UserAttributionRule userIdRule1 =
        UserAttributionRule.newBuilder()
            .setId("rule-id-1")
            .setData(
                UserAttributionRuleData.newBuilder()
                    .putCustomTokenRules(
                        "test.1",
                        UserAttributionTokenRule.newBuilder()
                            .setTokenObfuscationStrategy(OBFUSCATION_STRATEGY_HASH)
                            .build())
                    .putCustomTokenRules("test.2", UserAttributionTokenRule.getDefaultInstance()))
            .build();
    UserAttributionRule userIdRule2 =
        UserAttributionRule.newBuilder()
            .setId("rule-id-2")
            .setData(
                UserAttributionRuleData.newBuilder()
                    .putCustomTokenRules("test.3", UserAttributionTokenRule.getDefaultInstance())
                    .putCustomTokenRules(
                        "test.4",
                        UserAttributionTokenRule.newBuilder()
                            .setTokenObfuscationStrategy(OBFUSCATION_STRATEGY_HASH)
                            .build()))
            .build();
    List<DataType> dataTypes =
        translator.translateUserAttributionRules(List.of(userIdRule1, userIdRule2));
    DataType expectedDataType1 =
        buildExpectedDataType(CUSTOM_ATTRIBUTE_PREFIX + "test.1", "rule-id-1");
    DataType expectedDataType2 =
        buildExpectedDataType(CUSTOM_ATTRIBUTE_PREFIX + "test.4", "rule-id-2");
    assertEquals(2, dataTypes.size());
    assertEquals(expectedDataType1, dataTypes.get(0));
    assertEquals(expectedDataType2, dataTypes.get(1));
  }

  private DataType buildExpectedDataType(String attributeKey, String ruleId) {
    return DataType.newBuilder()
        .setDataTypeId(ruleId)
        .setTransformation(DATA_TRANSFORMATION_OBFUSCATE)
        .addMatchRules(
            DataType.DataTypeMatchRule.newBuilder()
                .setResult(RESULT_MATCH)
                .setSpanFilter(
                    SpanFilter.newBuilder()
                        .addRequiredMatchingAttributes(
                            AttributePredicate.newBuilder()
                                .setNamePredicate(
                                    StringPredicate.newBuilder()
                                        .setValue(attributeKey + RULE_ATTRIBUTE_KEY_SUFFIX)
                                        .setOperator(OPERATOR_EQUALS))
                                .setValuePredicate(
                                    StringPredicate.newBuilder()
                                        .setValue(ruleId)
                                        .setOperator(OPERATOR_EQUALS))))
                .setAttributeFilter(AttributeFilter.newBuilder().addPrefixes(attributeKey))
                .setPathPredicate(
                    PathPredicate.newBuilder()
                        .setPathSegmentPredicate(
                            StringPredicate.newBuilder()
                                .setValue(attributeKey)
                                .setOperator(OPERATOR_EQUALS))))
        .build();
  }
}
