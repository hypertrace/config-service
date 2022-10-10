package ai.traceable.userattribution.config.service.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.userattribution.config.service.v1.Authentication;
import ai.traceable.userattribution.config.service.v1.CreateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.DeleteUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v1.RankUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UpdateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.BasicAuthenticationUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.CustomJsonUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.CustomTokenRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.CustomUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.EncodedLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.HeaderLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.JwtUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RequestHeaderUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.ResponseBodyUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RuleCondition;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.util.Objects;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class UserAttributionConfigRequestValidatorTest {

  private static final String TEST_TENANT_ID =
      "UserAttributionConfigRequestValidatorTest-tenant-id";
  private final UserAttributionConfigRequestValidator validator =
      new UserAttributionConfigRequestValidator(new AuthenticationValidator());

  @Mock private RequestContext mockRequestContext;

  @Test
  void validatesGetRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetUserAttributionRulesRequest.newBuilder().build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetUserAttributionRulesRequest.newBuilder().build()));
  }

  @Test
  void validatesDeleteRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteUserAttributionRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "DeleteUserAttributionRuleRequest.rule_id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                DeleteUserAttributionRuleRequest.newBuilder().setRuleId("").build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                DeleteUserAttributionRuleRequest.newBuilder().setRuleId("rule-id").build()));
  }

  @Test
  void validatesCreateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateUserAttributionRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "CreateUserAttributionRuleRequest.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder().setName("").build()));

    assertInvalidArgStatusContaining(
        "Unexpected data case",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder().setName("rule-name").build()));

    assertInvalidArgStatusContaining(
        "CustomUserAttributionRuleData.yaml",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomData(CustomUserAttributionRuleData.newBuilder()))
                    .build()));

    assertInvalidArgStatusContaining(
        "Custom scope should not be empty",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomData(
                                CustomUserAttributionRuleData.newBuilder().setYaml("key: value")))
                    .setScope(
                        UserAttributionRuleScope.newBuilder()
                            .setCustomScope(
                                UserAttributionRuleScope.CustomScope.getDefaultInstance()))
                    .build()));

    assertInvalidArgStatusContaining(
        "Environment should not be empty",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomData(
                                CustomUserAttributionRuleData.newBuilder().setYaml("key: value")))
                    .setScope(
                        UserAttributionRuleScope.newBuilder()
                            .setCustomScope(
                                UserAttributionRuleScope.CustomScope.newBuilder()
                                    .addEnvironmentScopes(
                                        UserAttributionRuleScope.EnvironmentScope
                                            .getDefaultInstance())
                                    .build()))
                    .build()));

    assertInvalidArgStatusContaining(
        "duplicate element",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomData(
                                CustomUserAttributionRuleData.newBuilder().setYaml("key: value")))
                    .setScope(
                        UserAttributionRuleScope.newBuilder()
                            .setCustomScope(
                                UserAttributionRuleScope.CustomScope.newBuilder()
                                    .addEnvironmentScopes(
                                        UserAttributionRuleScope.EnvironmentScope.newBuilder()
                                            .setEnvironmentName("env"))
                                    .addEnvironmentScopes(
                                        UserAttributionRuleScope.EnvironmentScope.newBuilder()
                                            .setEnvironmentName("env"))
                                    .build()))
                    .build()));

    assertInvalidArgStatusContaining(
        "duplicate element",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomData(
                                CustomUserAttributionRuleData.newBuilder().setYaml("key: value")))
                    .setScope(
                        UserAttributionRuleScope.newBuilder()
                            .setCustomScope(
                                UserAttributionRuleScope.CustomScope.newBuilder()
                                    .addUrlScopes(
                                        UserAttributionRuleScope.UrlScope.newBuilder()
                                            .setUrlMatchRegex("regex")
                                            .build())
                                    .addUrlScopes(
                                        UserAttributionRuleScope.UrlScope.newBuilder()
                                            .setUrlMatchRegex("regex")
                                            .build())
                                    .build()))
                    .build()));

    assertInvalidArgStatusContaining(
        "Url regex should not be empty",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomData(
                                CustomUserAttributionRuleData.newBuilder().setYaml("key: value")))
                    .setScope(
                        UserAttributionRuleScope.newBuilder()
                            .setCustomScope(
                                UserAttributionRuleScope.CustomScope.newBuilder()
                                    .addUrlScopes(
                                        UserAttributionRuleScope.UrlScope.getDefaultInstance())
                                    .build())
                            .build())
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid url regex",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomData(
                                CustomUserAttributionRuleData.newBuilder().setYaml("key: value")))
                    .setScope(
                        UserAttributionRuleScope.newBuilder()
                            .setCustomScope(
                                UserAttributionRuleScope.CustomScope.newBuilder()
                                    .addUrlScopes(
                                        UserAttributionRuleScope.UrlScope.newBuilder()
                                            .setUrlMatchRegex("[")
                                            .build())
                                    .build())
                            .build())
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomData(
                                CustomUserAttributionRuleData.newBuilder().setYaml("key: value")))
                    .setScope(
                        UserAttributionRuleScope.newBuilder()
                            .setCustomScope(
                                UserAttributionRuleScope.CustomScope.newBuilder()
                                    .addEnvironmentScopes(
                                        UserAttributionRuleScope.EnvironmentScope.newBuilder()
                                            .setEnvironmentName("env")))
                            .build())
                    .build()));
  }

  @Test
  void validatesUpdateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateUserAttributionRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "UserAttributionRule.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateUserAttributionRuleRequest.newBuilder().build()));

    assertInvalidArgStatusContaining(
        "UserAttributionRule.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateUserAttributionRuleRequest.newBuilder()
                    .setRule(UserAttributionRule.newBuilder().setId("rule-id"))
                    .build()));

    assertInvalidArgStatusContaining(
        "Unexpected data case",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateUserAttributionRuleRequest.newBuilder()
                    .setRule(UserAttributionRule.newBuilder().setName("rule-name").setId("rule-id"))
                    .build()));

    assertInvalidArgStatusContaining(
        "CustomUserAttributionRuleData.yaml",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateUserAttributionRuleRequest.newBuilder()
                    .setRule(
                        UserAttributionRule.newBuilder()
                            .setName("rule-name")
                            .setId("rule-id")
                            .setData(
                                UserAttributionRuleData.newBuilder()
                                    .setCustomData(CustomUserAttributionRuleData.newBuilder())))
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateUserAttributionRuleRequest.newBuilder()
                    .setRule(
                        UserAttributionRule.newBuilder()
                            .setName("rule-name")
                            .setId("rule-id")
                            .setData(
                                UserAttributionRuleData.newBuilder()
                                    .setCustomData(
                                        CustomUserAttributionRuleData.newBuilder()
                                            .setYaml("key: value"))))
                    .build()));
  }

  @Test
  void validatesResponseBodyCreate() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "rule condition",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setResponseBodyData(
                                ResponseBodyUserAttributionRuleData.getDefaultInstance()))
                    .build()));

    assertInvalidArgStatusContaining(
        "encoded location",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setResponseBodyData(
                                ResponseBodyUserAttributionRuleData.newBuilder()
                                    .setCondition(
                                        RuleCondition.newBuilder().setUrlMatchRegex("regex"))))
                    .build()));

    assertInvalidArgStatusContaining(
        "Authentication type cannot be empty",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setResponseBodyData(
                                ResponseBodyUserAttributionRuleData.newBuilder()
                                    .setCondition(
                                        RuleCondition.newBuilder().setUrlMatchRegex("regex"))
                                    .setUserIdLocation(
                                        EncodedLocation.newBuilder().setJsonPath("some-path"))
                                    .setAuthentication(Authentication.newBuilder())))
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setResponseBodyData(
                                ResponseBodyUserAttributionRuleData.newBuilder()
                                    .setCondition(
                                        RuleCondition.newBuilder().setUrlMatchRegex("regex"))
                                    .setUserIdLocation(
                                        EncodedLocation.newBuilder().setJsonPath("some-path"))
                                    .setAuthentication(
                                        Authentication.newBuilder().setType("Auth-type"))))
                    .build()));
  }

  @Test
  void validatesCustomTokenCreate() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "header location",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomTokenData(
                                CustomTokenRuleData.newBuilder()
                                    .setRequestHeaderLocation(HeaderLocation.getDefaultInstance())))
                    .build()));

    assertInvalidArgStatusContaining(
        "encoded location",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomTokenData(
                                CustomTokenRuleData.newBuilder()
                                    .setRequestBodyLocation(EncodedLocation.getDefaultInstance())))
                    .build()));

    assertInvalidArgStatusContaining(
        "Authentication type cannot be empty",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomTokenData(
                                CustomTokenRuleData.newBuilder()
                                    .setRequestHeaderLocation(
                                        HeaderLocation.newBuilder().setHeaderName("some-header"))
                                    .setAuthentication(Authentication.newBuilder())))
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomTokenData(
                                CustomTokenRuleData.newBuilder()
                                    .setRequestHeaderLocation(
                                        HeaderLocation.newBuilder().setHeaderName("some-header"))
                                    .setAuthentication(
                                        Authentication.newBuilder().setType("OAuth 2.0"))))
                    .build()));
  }

  @Test
  void validatesRequestHeaderCreate() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "header location",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setRequestHeaderData(
                                RequestHeaderUserAttributionRuleData.getDefaultInstance()))
                    .build()));

    assertInvalidArgStatusContaining(
        "Authentication type cannot be empty",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setRequestHeaderData(
                                RequestHeaderUserAttributionRuleData.newBuilder()
                                    .setUserIdLocation(
                                        HeaderLocation.newBuilder().setHeaderName("some-header"))
                                    .setAuthentication(Authentication.newBuilder())))
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setRequestHeaderData(
                                RequestHeaderUserAttributionRuleData.newBuilder()
                                    .setUserIdLocation(
                                        HeaderLocation.newBuilder().setHeaderName("some-header"))
                                    .setAuthentication(
                                        Authentication.newBuilder().setType("OAuth 2.0"))))
                    .build()));
  }

  @Test
  void validatesBasicAuthCreate() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setBasicAuthenticationData(
                                BasicAuthenticationUserAttributionRuleData.getDefaultInstance()))
                    .build()));
  }

  @Test
  void validateRulesWithCaptureGroupCreate() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "Regex should have exactly one capture group but found 5 capture groups",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setRequestHeaderData(
                                RequestHeaderUserAttributionRuleData.newBuilder()
                                    .setUserIdLocation(
                                        HeaderLocation.newBuilder()
                                            .setHeaderName("some-header")
                                            .setParsingTarget(
                                                UserAttributionRuleData.ParsingTarget.newBuilder()
                                                    .setRegexCaptureGroup(
                                                        "/(1(2(3)))(?:A)()4[(C)][(]D[)]\\(E\\)(5)/")
                                                    .build()))))
                    .build()));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setJwtData(
                                JwtUserAttributionRuleData.newBuilder()
                                    .setJwtLocation(
                                        HeaderLocation.newBuilder().setHeaderName("jwt"))
                                    .setUserIdLocation(
                                        EncodedLocation.newBuilder()
                                            .setJsonPath("userid")
                                            .setParsingTarget(
                                                UserAttributionRuleData.ParsingTarget.newBuilder()
                                                    .setRegexCaptureGroup("\\d+(.*)")
                                                    .build()))
                                    .setUserIdClaim("user-id-claim")))
                    .build()));
  }

  @Test
  void validatesJwtCreate() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "header location",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setJwtData(JwtUserAttributionRuleData.newBuilder()))
                    .build()));
    assertInvalidArgStatusContaining(
        "JwtUserAttributionRuleData.user_id_claim",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setJwtData(
                                JwtUserAttributionRuleData.newBuilder()
                                    .setJwtLocation(
                                        HeaderLocation.newBuilder().setCookieName("jwt"))))
                    .build()));
    assertInvalidArgStatusContaining(
        "Authentication type cannot be empty",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setJwtData(
                                JwtUserAttributionRuleData.newBuilder()
                                    .setJwtLocation(
                                        HeaderLocation.newBuilder().setHeaderName("jwt"))
                                    .setUserIdClaim("user-id-claim")
                                    .setAuthentication(Authentication.newBuilder())))
                    .build()));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setJwtData(
                                JwtUserAttributionRuleData.newBuilder()
                                    .setJwtLocation(
                                        HeaderLocation.newBuilder().setHeaderName("jwt"))
                                    .setUserIdClaim("user-id-claim")
                                    .setAuthentication(
                                        Authentication.newBuilder().setType("Bearer Token"))))
                    .build()));
  }

  @Test
  void validatesRankRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, RankUserAttributionRuleRequest.newBuilder().build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "id_to_update",
        () ->
            validator.validateOrThrow(
                mockRequestContext, RankUserAttributionRuleRequest.newBuilder().build()));

    assertInvalidArgStatusContaining(
        "Can't rerank a rule against itself",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                RankUserAttributionRuleRequest.newBuilder()
                    .setIdToUpdate("some-id")
                    .setPrecedingRuleId("some-id")
                    .build()));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                RankUserAttributionRuleRequest.newBuilder().setIdToUpdate("some-id").build()));
  }

  @Test
  void validatesRuleUpdate() {
    UserAttributionRule existingRule = UserAttributionRule.newBuilder().setRank(2).build();
    UserAttributionRule updatedRule = UserAttributionRule.newBuilder().setRank(3).build();
    assertInvalidArgStatusContaining(
        "rank", () -> validator.validateUpdateOrThrow(existingRule, updatedRule));

    assertDoesNotThrow(
        () ->
            validator.validateUpdateOrThrow(
                existingRule, updatedRule.toBuilder().setRank(existingRule.getRank()).build()));
  }

  @Test
  void validatesCustomRuleYaml() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "YAML of unexpected type",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomData(
                                CustomUserAttributionRuleData.newBuilder().setYaml("bad-yaml")))
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomData(
                                CustomUserAttributionRuleData.newBuilder().setYaml("[]")))
                    .build()));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomData(
                                CustomUserAttributionRuleData.newBuilder().setYaml("key: value")))
                    .build()));
  }

  @Test
  void validatesCustomJsonRule() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "JSON rule data should be specified for at least one of the target attributes",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomJsonData(CustomJsonUserAttributionRuleData.newBuilder()))
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid JSON format",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomJsonData(
                                CustomJsonUserAttributionRuleData.newBuilder()
                                    .setUserIdRuleData("bad-json")))
                    .build()));

    String validJsonRuleData =
        "{\n"
            + "  \"projector\": {\n"
            + "    \"attributeProjector\": {\n"
            + "      \"attributeKey\": \"http.request.header.authorization\",\n"
            + "      \"attributeRule\": {\n"
            + "        \"initialActions\": [\n"
            + "          {\n"
            + "            \"attributeArrayAppend\": {\n"
            + "              \"attributeKey\": \"traceableai.auth.types\",\n"
            + "              \"valueProjectionRule\": {\n"
            + "                \"projector\": {\n"
            + "                  \"valueProjector\": {\n"
            + "                    \"value\": \"Basic\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          }\n"
            + "        ]\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomJsonData(
                                CustomJsonUserAttributionRuleData.newBuilder()
                                    .setAuthTypeRuleData(validJsonRuleData)))
                    .build()));
  }

  private void assertInvalidArgStatusContaining(String text, Executable executable) {
    StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, executable);
    assertEquals(Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    assertTrue(
        Objects.requireNonNull(exception.getStatus().getDescription()).contains(text),
        "Expected arg to contain " + text + " but was " + exception.getMessage());
  }
}
