package ai.traceable.userattribution.config.service.v2.migration;

import static ai.traceable.userattribution.config.service.v2.migration.UserAttributionRuleConverter.BASIC_AUTH_TYPE;
import static ai.traceable.userattribution.config.service.v2.migration.UserAttributionRuleConverter.BASIC_TEMPLATE;
import static ai.traceable.userattribution.config.service.v2.migration.UserAttributionRuleConverter.CUSTOM_TEMPLATE;
import static ai.traceable.userattribution.config.service.v2.migration.UserAttributionRuleConverter.DEFAULT_BASIC_AUTHORIZATION_HEADER_NAME;
import static ai.traceable.userattribution.config.service.v2.migration.UserAttributionRuleConverter.DEFAULT_BASIC_AUTHORIZATION_REGEX_CAPTURE_GROUP;
import static ai.traceable.userattribution.config.service.v2.migration.UserAttributionRuleConverter.DEFAULT_BASIC_AUTHORIZATION_USERNAME_REGEX_CAPTURE_GROUP;
import static ai.traceable.userattribution.config.service.v2.migration.UserAttributionRuleConverter.JWT_TEMPLATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.userattribution.config.service.v1.Authentication;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.BasicAuthenticationUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope.CustomScope;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope.EnvironmentScope;
import ai.traceable.userattribution.config.service.v2.Attribute;
import ai.traceable.userattribution.config.service.v2.AttributeProjection;
import ai.traceable.userattribution.config.service.v2.CustomProjection;
import ai.traceable.userattribution.config.service.v2.KeyMatch;
import ai.traceable.userattribution.config.service.v2.LiteralValue;
import ai.traceable.userattribution.config.service.v2.LiteralValueProjection;
import ai.traceable.userattribution.config.service.v2.MatchCondition;
import ai.traceable.userattribution.config.service.v2.MatchOperator;
import ai.traceable.userattribution.config.service.v2.PayloadMatch;
import ai.traceable.userattribution.config.service.v2.Predicate;
import ai.traceable.userattribution.config.service.v2.RootRelativeProjection;
import ai.traceable.userattribution.config.service.v2.StringList;
import ai.traceable.userattribution.config.service.v2.UserAttributionRootTokenRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionTokenRule;
import ai.traceable.userattribution.config.service.v2.ValueProjection;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class UserAttributionRuleConverterTest {
  private static final String RULE_ID = "test-rule-id";
  private static final String RULE_NAME = "test-rule-name";
  private static final int RULE_RANK = 1;
  private static final boolean RULE_DISABLED = true;
  private static final String TEST_ENVIRONMENT = "test-env";

  static UserAttributionRuleConverter converter;

  @BeforeAll
  static void setup() {
    converter = new UserAttributionRuleConverter();
  }

  @Test
  void convertBasicAuth() {
    UserAttributionRuleData data =
        UserAttributionRuleData.newBuilder()
            .setBasicAuthenticationData(
                BasicAuthenticationUserAttributionRuleData.getDefaultInstance())
            .build();
    UserAttributionRule userAttributionRule = buildRuleV1(data);
    Optional<ai.traceable.userattribution.config.service.v2.UserAttributionRule> convertedRule =
        converter.convert(userAttributionRule);
    ai.traceable.userattribution.config.service.v2.UserAttributionRule expectedRule =
        buildRuleV2(
            BASIC_TEMPLATE,
            null,
            buildAttributeProjectionTokenRuleV2(
                DEFAULT_BASIC_AUTHORIZATION_HEADER_NAME,
                List.of(
                    buildRegexCaptureGroupValueProjection(
                        DEFAULT_BASIC_AUTHORIZATION_REGEX_CAPTURE_GROUP),
                    buildBase64ValueProjection(),
                    buildRegexCaptureGroupValueProjection(
                        DEFAULT_BASIC_AUTHORIZATION_USERNAME_REGEX_CAPTURE_GROUP))),
            buildLiteralValueProjectionTokenRuleV2(BASIC_AUTH_TYPE));
    assertTrue(convertedRule.isPresent());
    assertEquals(expectedRule, convertedRule.get());
  }

  @Test
  void convertJwtAuth() {
    UserAttributionRuleData data =
        UserAttributionRuleData.newBuilder()
            .setJwtData(
                UserAttributionRuleData.JwtUserAttributionRuleData.newBuilder()
                    .setAuthentication(Authentication.newBuilder().setType("OAUTH 2.0"))
                    .setJwtLocation(
                        UserAttributionRuleData.HeaderLocation.newBuilder()
                            .setHeaderName("jwt")
                            .setParsingTarget(
                                UserAttributionRuleData.ParsingTarget.newBuilder()
                                    .setRegexCaptureGroup("Bearer (.*)")))
                    .setUserIdClaim("email"))
            .build();
    UserAttributionRule userAttributionRule = buildRuleV1(data);
    Optional<ai.traceable.userattribution.config.service.v2.UserAttributionRule> convertedRule =
        converter.convert(userAttributionRule);
    ai.traceable.userattribution.config.service.v2.UserAttributionRule expectedRule =
        buildRuleV2(
            JWT_TEMPLATE,
            buildAttributeProjectionRootTokenRuleV2(
                "jwt", List.of(buildRegexCaptureGroupValueProjection("Bearer (.*)"))),
            buildRootRelativeProjectionTokenRuleV2(List.of(buildJwtClaimValueProjection("email"))),
            buildLiteralValueProjectionTokenRuleV2("OAUTH 2.0"));
    assertTrue(convertedRule.isPresent());
    assertEquals(expectedRule, convertedRule.get());
  }

  @Test
  void convertRequestHeaderAuth() {
    UserAttributionRuleData data =
        UserAttributionRuleData.newBuilder()
            .setRequestHeaderData(
                UserAttributionRuleData.RequestHeaderUserAttributionRuleData.newBuilder()
                    .setAuthentication(Authentication.newBuilder().setType("OAUTH 2.0"))
                    .setUserIdLocation(
                        UserAttributionRuleData.HeaderLocation.newBuilder()
                            .setHeaderName("jwt")
                            .setParsingTarget(
                                UserAttributionRuleData.ParsingTarget.newBuilder()
                                    .setRegexCaptureGroup("Bearer (.*)"))))
            .build();
    UserAttributionRule userAttributionRule = buildRuleV1(data);
    Optional<ai.traceable.userattribution.config.service.v2.UserAttributionRule> convertedRule =
        converter.convert(userAttributionRule);
    ai.traceable.userattribution.config.service.v2.UserAttributionRule expectedRule =
        buildRuleV2(
            CUSTOM_TEMPLATE,
            null,
            buildAttributeProjectionTokenRuleV2(
                "jwt", List.of(buildRegexCaptureGroupValueProjection("Bearer (.*)"))),
            buildLiteralValueProjectionTokenRuleV2("OAUTH 2.0"));
    assertTrue(convertedRule.isPresent());
    assertEquals(expectedRule, convertedRule.get());
  }

  @Test
  void convertResponseBodyAuth() {
    UserAttributionRuleData data =
        UserAttributionRuleData.newBuilder()
            .setResponseBodyData(
                UserAttributionRuleData.ResponseBodyUserAttributionRuleData.newBuilder()
                    .setAuthentication(Authentication.newBuilder().setType("OAUTH 2.0"))
                    .setUserIdLocation(
                        UserAttributionRuleData.EncodedLocation.newBuilder()
                            .setJsonPath("$.user.email")
                            .setParsingTarget(
                                UserAttributionRuleData.ParsingTarget.newBuilder()
                                    .setRegexCaptureGroup("(.*)@"))))
            .build();
    UserAttributionRule userAttributionRule = buildRuleV1(data);
    Optional<ai.traceable.userattribution.config.service.v2.UserAttributionRule> convertedRule =
        converter.convert(userAttributionRule);
    ai.traceable.userattribution.config.service.v2.UserAttributionRule expectedRule =
        buildRuleV2(
            CUSTOM_TEMPLATE,
            null,
            buildAttributeProjectionTokenRuleV2(
                List.of(
                    buildJsonPathValueProjection("$.user.email"),
                    buildRegexCaptureGroupValueProjection("(.*)@"))),
            buildLiteralValueProjectionTokenRuleV2("OAUTH 2.0"));
    assertTrue(convertedRule.isPresent());
    assertEquals(expectedRule, convertedRule.get());
  }

  @Test
  void convertCustomJsonAuth() {
    UserAttributionRuleData data =
        UserAttributionRuleData.newBuilder()
            .setCustomJsonData(
                UserAttributionRuleData.CustomJsonUserAttributionRuleData.newBuilder()
                    .setAuthTypeRuleData("Custom json"))
            .build();
    UserAttributionRule userAttributionRule = buildRuleV1(data);
    Optional<ai.traceable.userattribution.config.service.v2.UserAttributionRule> convertedRule =
        converter.convert(userAttributionRule);
    ai.traceable.userattribution.config.service.v2.UserAttributionRule expectedRule =
        buildRuleV2(CUSTOM_TEMPLATE, null, null, buildCustomProjectionTokenRuleV2("Custom json"));
    assertTrue(convertedRule.isPresent());
    assertEquals(expectedRule, convertedRule.get());
  }

  @Test
  void convertCustomTokenAuth() {
    UserAttributionRuleData data =
        UserAttributionRuleData.newBuilder()
            .setCustomTokenData(
                UserAttributionRuleData.CustomTokenRuleData.newBuilder()
                    .setAuthentication(Authentication.newBuilder().setType("OAUTH 2.0"))
                    .setRequestHeaderLocation(
                        UserAttributionRuleData.HeaderLocation.newBuilder().setHeaderName("jwt")))
            .build();
    UserAttributionRule userAttributionRule = buildRuleV1(data);
    Optional<ai.traceable.userattribution.config.service.v2.UserAttributionRule> convertedRule =
        converter.convert(userAttributionRule);
    UserAttributionTokenRule authRule =
        UserAttributionTokenRule.newBuilder()
            .setTokenConditionalPredicate(
                Predicate.newBuilder()
                    .setAttributePredicate(
                        Predicate.AttributePredicate.newBuilder()
                            .setAttribute(
                                Attribute.newBuilder()
                                    .setRequestHeader(
                                        KeyMatch.newBuilder()
                                            .setArgument("jwt")
                                            .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)))
                            .setAttributeValueMatchCondition(
                                MatchCondition.newBuilder()
                                    .setOperator(MatchOperator.MATCH_OPERATOR_NOT_EQUALS)
                                    .setMatchValue(
                                        LiteralValue.newBuilder()
                                            .setNullValue(
                                                LiteralValue.NullValue.getDefaultInstance())))))
            .setLiteralValueProjection(
                LiteralValueProjection.newBuilder()
                    .setLiteralValue(LiteralValue.newBuilder().setStringValue("OAUTH 2.0")))
            .build();
    ai.traceable.userattribution.config.service.v2.UserAttributionRule expectedRule =
        buildRuleV2(CUSTOM_TEMPLATE, null, null, authRule);
    assertTrue(convertedRule.isPresent());
    assertEquals(expectedRule, convertedRule.get());
  }

  private UserAttributionRule buildRuleV1(UserAttributionRuleData data) {
    return UserAttributionRule.newBuilder()
        .setId(RULE_ID)
        .setName(RULE_NAME)
        .setDisabled(RULE_DISABLED)
        .setRank(RULE_RANK)
        .setScope(
            UserAttributionRuleScope.newBuilder()
                .setCustomScope(
                    CustomScope.newBuilder()
                        .addEnvironmentScopes(
                            EnvironmentScope.newBuilder()
                                .setEnvironmentName(TEST_ENVIRONMENT)
                                .build())))
        .setData(data)
        .build();
  }

  private ai.traceable.userattribution.config.service.v2.UserAttributionRule buildRuleV2(
      String template,
      ai.traceable.userattribution.config.service.v2.UserAttributionRootTokenRule rootTokenRule,
      ai.traceable.userattribution.config.service.v2.UserAttributionTokenRule userIdRule,
      ai.traceable.userattribution.config.service.v2.UserAttributionTokenRule authTypeRule) {
    ai.traceable.userattribution.config.service.v2.UserAttributionRuleData.Builder dataBuilder =
        ai.traceable.userattribution.config.service.v2.UserAttributionRuleData.newBuilder();
    dataBuilder.setName(RULE_NAME);
    dataBuilder.setDisabled(RULE_DISABLED);
    dataBuilder.setTemplate(template);
    dataBuilder.setScope(
        ai.traceable.userattribution.config.service.v2.UserAttributionRuleScope.newBuilder()
            .setEnvironmentScope(
                ai.traceable.userattribution.config.service.v2.EnvironmentScope.newBuilder()
                    .setEnvironmentNames(StringList.newBuilder().addValues(TEST_ENVIRONMENT))));
    Optional.ofNullable(rootTokenRule).ifPresent(dataBuilder::setRootTokenRule);
    Optional.ofNullable(userIdRule).ifPresent(dataBuilder::setUserIdRule);
    Optional.ofNullable(authTypeRule).ifPresent(dataBuilder::setAuthTypeRule);
    return ai.traceable.userattribution.config.service.v2.UserAttributionRule.newBuilder()
        .setId(RULE_ID)
        .setData(dataBuilder)
        .setRank(RULE_RANK)
        .build();
  }

  private UserAttributionRootTokenRule buildAttributeProjectionRootTokenRuleV2(
      String keyName, List<ValueProjection> valueProjections) {
    return UserAttributionRootTokenRule.newBuilder()
        .setAttributeProjection(buildAttributeProjectionV2(keyName, valueProjections))
        .build();
  }

  private UserAttributionTokenRule buildAttributeProjectionTokenRuleV2(
      String keyName, List<ValueProjection> valueProjections) {
    return UserAttributionTokenRule.newBuilder()
        .setAttributeProjection(buildAttributeProjectionV2(keyName, valueProjections))
        .build();
  }

  private UserAttributionTokenRule buildAttributeProjectionTokenRuleV2(
      List<ValueProjection> valueProjections) {
    return UserAttributionTokenRule.newBuilder()
        .setAttributeProjection(
            AttributeProjection.newBuilder()
                .setAttribute(
                    Attribute.newBuilder().setResponseBody(PayloadMatch.getDefaultInstance()))
                .addAllValueProjections(valueProjections))
        .build();
  }

  private AttributeProjection buildAttributeProjectionV2(
      String keyName, List<ValueProjection> valueProjections) {
    return AttributeProjection.newBuilder()
        .setAttribute(
            Attribute.newBuilder()
                .setRequestHeader(
                    KeyMatch.newBuilder()
                        .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                        .setArgument(keyName)))
        .addAllValueProjections(valueProjections)
        .build();
  }

  private UserAttributionTokenRule buildCustomProjectionTokenRuleV2(String jsonData) {
    return UserAttributionTokenRule.newBuilder()
        .setCustomProjection(CustomProjection.newBuilder().setCustomProjection(jsonData))
        .build();
  }

  private UserAttributionTokenRule buildLiteralValueProjectionTokenRuleV2(String literalValue) {
    return UserAttributionTokenRule.newBuilder()
        .setLiteralValueProjection(
            LiteralValueProjection.newBuilder()
                .setLiteralValue(LiteralValue.newBuilder().setStringValue(literalValue)))
        .build();
  }

  private UserAttributionTokenRule buildRootRelativeProjectionTokenRuleV2(
      List<ValueProjection> valueProjections) {
    return UserAttributionTokenRule.newBuilder()
        .setRootRelativeProjection(
            RootRelativeProjection.newBuilder().addAllValueProjections(valueProjections))
        .build();
  }

  private ValueProjection buildJwtClaimValueProjection(String claimKey) {
    return ValueProjection.newBuilder()
        .setJwtPayloadClaim(
            ValueProjection.JwtPayloadClaimProjection.newBuilder().setClaimKey(claimKey))
        .build();
  }

  private ValueProjection buildJsonPathValueProjection(String jsonPath) {
    return ValueProjection.newBuilder()
        .setJsonPath(ValueProjection.JsonPathProjection.newBuilder().setPath(jsonPath))
        .build();
  }

  private ValueProjection buildBase64ValueProjection() {
    return ValueProjection.newBuilder()
        .setBase64Projection(ValueProjection.Base64Projection.getDefaultInstance())
        .build();
  }

  private ValueProjection buildRegexCaptureGroupValueProjection(String regexCaptureGroup) {
    return ValueProjection.newBuilder()
        .setRegexCaptureGroup(
            ValueProjection.RegexCaptureGroupProjection.newBuilder()
                .setRegexCaptureGroup(regexCaptureGroup))
        .build();
  }
}
