package ai.traceable.sessionattribution.config.service.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.sessionattribution.config.service.v1.AttributeProjection;
import ai.traceable.sessionattribution.config.service.v1.CreateSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.v1.DeleteSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.v1.LiteralValue;
import ai.traceable.sessionattribution.config.service.v1.MatchCondition;
import ai.traceable.sessionattribution.config.service.v1.MatchOperator;
import ai.traceable.sessionattribution.config.service.v1.RankSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.v1.RequestAttributeKeyLocation;
import ai.traceable.sessionattribution.config.service.v1.RequestSessionTokenDetails;
import ai.traceable.sessionattribution.config.service.v1.SessionAttributionRuleScope;
import ai.traceable.sessionattribution.config.service.v1.SessionTokenRule;
import ai.traceable.sessionattribution.config.service.v1.SessionTokenValueRule;
import ai.traceable.sessionattribution.config.service.v1.UpdateSessionAttributionRuleRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SessionAttributionConfigRequestValidatorTest {
  private static final String TEST_TENANT_ID =
      "SessionAttributionConfigRequestValidatorTest-tenant-id";
  private static final SessionTokenRule RULE =
      SessionTokenRule.newBuilder()
          .setRequestSessionTokenDetails(
              RequestSessionTokenDetails.newBuilder()
                  .setTokenLocation(
                      RequestAttributeKeyLocation.REQUEST_ATTRIBUTE_KEY_LOCATION_HEADER)
                  .build())
          .setTokenValueRule(
              SessionTokenValueRule.newBuilder()
                  .setTokenValueProjection(
                      AttributeProjection.newBuilder()
                          .setAttributeKeyMatchCondition(
                              MatchCondition.newBuilder()
                                  .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                  .setMatchValue(
                                      LiteralValue.newBuilder().setStringValue("authorization"))))
                  .build())
          .build();
  @InjectMocks SessionAttributionConfigRequestValidator validator;
  @Mock SessionTokenRuleValidator tokenRuleValidator;
  @Mock private RequestContext mockRequestContext;

  @Test
  void validatesGetRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID", () -> validator.validateGetRequest(mockRequestContext));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(() -> validator.validateGetRequest(mockRequestContext));
  }

  @Test
  void validatesDeleteRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateDeleteRequest(
                mockRequestContext, DeleteSessionAttributionRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "DeleteSessionAttributionRuleRequest.id",
        () ->
            validator.validateDeleteRequest(
                mockRequestContext,
                DeleteSessionAttributionRuleRequest.newBuilder().setId("").build()));

    assertDoesNotThrow(
        () ->
            validator.validateDeleteRequest(
                mockRequestContext,
                DeleteSessionAttributionRuleRequest.newBuilder().setId("rule-id").build()));
  }

  @Test
  void validateCreate_empty_name() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "CreateSessionAttributionRuleRequest.name",
        () ->
            validator.validateCreateRequest(
                mockRequestContext,
                CreateSessionAttributionRuleRequest.newBuilder().addTokenRules(RULE).build()));
  }

  @Test
  void validateCreate_scope() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "duplicate element: env",
        () ->
            validator.validateCreateRequest(
                mockRequestContext,
                CreateSessionAttributionRuleRequest.newBuilder()
                    .setName("rule")
                    .setScope(
                        SessionAttributionRuleScope.newBuilder()
                            .addAllEnvironmentNames(List.of("env", "env")))
                    .addTokenRules(RULE)
                    .build()));
    assertInvalidArgStatusContaining(
        "duplicate element: src",
        () ->
            validator.validateCreateRequest(
                mockRequestContext,
                CreateSessionAttributionRuleRequest.newBuilder()
                    .setName("rule")
                    .setScope(
                        SessionAttributionRuleScope.newBuilder()
                            .addAllServiceNameRegexes(List.of("src", "src")))
                    .addTokenRules(RULE)
                    .build()));
    assertInvalidArgStatusContaining(
        "duplicate element: url",
        () ->
            validator.validateCreateRequest(
                mockRequestContext,
                CreateSessionAttributionRuleRequest.newBuilder()
                    .setName("rule")
                    .setScope(
                        SessionAttributionRuleScope.newBuilder()
                            .addAllUrlMatchRegexes(List.of("url", "url")))
                    .addTokenRules(RULE)
                    .build()));
  }

  @Test
  void validateUpdate_empty_name() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "UpdateSessionAttributionRuleRequest.name",
        () ->
            validator.validateUpdateRequest(
                mockRequestContext,
                UpdateSessionAttributionRuleRequest.newBuilder()
                    .setId("id")
                    .addTokenRules(RULE)
                    .build()));
  }

  @Test
  void validateUpdate_empty_id() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "UpdateSessionAttributionRuleRequest.id",
        () ->
            validator.validateUpdateRequest(
                mockRequestContext,
                UpdateSessionAttributionRuleRequest.newBuilder().addTokenRules(RULE).build()));
  }

  @Test
  void validateUpdate_scope() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "One of the scope must be non empty",
        () ->
            validator.validateUpdateRequest(
                mockRequestContext,
                UpdateSessionAttributionRuleRequest.newBuilder()
                    .setId("id")
                    .setName("rule")
                    .setScope(SessionAttributionRuleScope.getDefaultInstance())
                    .addTokenRules(RULE)
                    .build()));
    assertInvalidArgStatusContaining(
        "duplicate element: env",
        () ->
            validator.validateUpdateRequest(
                mockRequestContext,
                UpdateSessionAttributionRuleRequest.newBuilder()
                    .setId("id")
                    .setName("rule")
                    .setScope(
                        SessionAttributionRuleScope.newBuilder()
                            .addAllEnvironmentNames(List.of("env", "env")))
                    .addTokenRules(RULE)
                    .build()));
    assertInvalidArgStatusContaining(
        "duplicate element: src",
        () ->
            validator.validateUpdateRequest(
                mockRequestContext,
                UpdateSessionAttributionRuleRequest.newBuilder()
                    .setId("id")
                    .setName("rule")
                    .setScope(
                        SessionAttributionRuleScope.newBuilder()
                            .addAllServiceNameRegexes(List.of("src", "src")))
                    .addTokenRules(RULE)
                    .build()));
    assertInvalidArgStatusContaining(
        "duplicate element: url",
        () ->
            validator.validateUpdateRequest(
                mockRequestContext,
                UpdateSessionAttributionRuleRequest.newBuilder()
                    .setId("id")
                    .setName("rule")
                    .setScope(
                        SessionAttributionRuleScope.newBuilder()
                            .addAllUrlMatchRegexes(List.of("url", "url")))
                    .addTokenRules(RULE)
                    .build()));

    assertInvalidArgStatusContaining(
        "One of the scope must be non empty",
        () ->
            validator.validateUpdateRequest(
                mockRequestContext,
                UpdateSessionAttributionRuleRequest.newBuilder()
                    .setId("id")
                    .setName("rule")
                    .setScope(SessionAttributionRuleScope.newBuilder())
                    .addTokenRules(RULE)
                    .build()));
  }

  @Test
  void validateRank() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "RankSessionAttributionRuleRequest.id_to_update",
        () ->
            validator.validateRankRequest(
                mockRequestContext, RankSessionAttributionRuleRequest.getDefaultInstance()));

    assertInvalidArgStatusContaining(
        "Can't rerank a rule against itself:",
        () ->
            validator.validateRankRequest(
                mockRequestContext,
                RankSessionAttributionRuleRequest.newBuilder()
                    .setIdToUpdate("id")
                    .setPrecedingRuleId("id")
                    .build()));
  }

  private void assertInvalidArgStatusContaining(String text, Executable executable) {
    StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, executable);
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    assertTrue(
        Objects.requireNonNull(exception.getStatus().getDescription()).contains(text),
        "Expected arg to contain " + text + " but was " + exception.getMessage());
  }
}
