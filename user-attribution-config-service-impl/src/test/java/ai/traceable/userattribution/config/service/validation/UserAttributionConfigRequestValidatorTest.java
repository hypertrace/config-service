package ai.traceable.userattribution.config.service.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.userattribution.config.service.v1.CreateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.DeleteUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v1.RankUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UpdateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.BasicAuthenticationUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.CustomUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.EncodedLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.HeaderLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.JwtUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RequestHeaderUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.ResponseBodyUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RuleCondition;
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
      new UserAttributionConfigRequestValidator();

  @Mock private RequestContext mockRequestContext;

  @Test
  void validatesGetRequest() {
    assertIllegalArgContaining(
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
    assertIllegalArgContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteUserAttributionRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertIllegalArgContaining(
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
    assertIllegalArgContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateUserAttributionRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertIllegalArgContaining(
        "CreateUserAttributionRuleRequest.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder().setName("").build()));

    assertIllegalArgContaining(
        "Unexpected data case",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder().setName("rule-name").build()));

    assertIllegalArgContaining(
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

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomData(
                                CustomUserAttributionRuleData.newBuilder().setYaml("yaml")))
                    .build()));
  }

  @Test
  void validatesUpdateRequest() {
    assertIllegalArgContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateUserAttributionRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertIllegalArgContaining(
        "UserAttributionRule.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateUserAttributionRuleRequest.newBuilder().build()));

    assertIllegalArgContaining(
        "UserAttributionRule.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateUserAttributionRuleRequest.newBuilder()
                    .setRule(UserAttributionRule.newBuilder().setId("rule-id"))
                    .build()));

    assertIllegalArgContaining(
        "Unexpected data case",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateUserAttributionRuleRequest.newBuilder()
                    .setRule(UserAttributionRule.newBuilder().setName("rule-name").setId("rule-id"))
                    .build()));

    assertIllegalArgContaining(
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
                                            .setYaml("yaml"))))
                    .build()));
  }

  @Test
  void validatesResponseBodyCreate() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertIllegalArgContaining(
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

    assertIllegalArgContaining(
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
                                        EncodedLocation.newBuilder().setJsonPath("some-path"))))
                    .build()));
  }

  @Test
  void validatesRequestHeaderCreate() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertIllegalArgContaining(
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
                                        HeaderLocation.newBuilder().setHeaderName("some-header"))))
                    .build()));
  }

  @Test
  void validatesBasicAuthCreate() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertIllegalArgContaining(
        "header location",
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

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("rule-name")
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setBasicAuthenticationData(
                                BasicAuthenticationUserAttributionRuleData.newBuilder()
                                    .setLocation(
                                        HeaderLocation.newBuilder().setHeaderName("name"))))
                    .build()));
  }

  @Test
  void validatesJwtCreate() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertIllegalArgContaining(
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
    assertIllegalArgContaining(
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
                                    .setUserIdClaim("user-id-claim")))
                    .build()));
  }

  @Test
  void validatesRankRequest() {
    assertIllegalArgContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, RankUserAttributionRuleRequest.newBuilder().build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertIllegalArgContaining(
        "id_to_update",
        () ->
            validator.validateOrThrow(
                mockRequestContext, RankUserAttributionRuleRequest.newBuilder().build()));

    assertIllegalArgContaining(
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
    assertIllegalArgContaining(
        "rank", () -> validator.validateUpdateOrThrow(existingRule, updatedRule));

    assertDoesNotThrow(
        () ->
            validator.validateUpdateOrThrow(
                existingRule, updatedRule.toBuilder().setRank(existingRule.getRank()).build()));
  }

  private void assertIllegalArgContaining(String text, Executable executable) {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, executable);
    assertTrue(
        exception.getMessage().contains(text),
        "Expected arg to contain " + text + " but was " + exception.getMessage());
  }
}
