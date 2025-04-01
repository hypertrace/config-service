package ai.traceable.ast.hooks.config.service.validators;

import static ai.traceable.ast.hooks.config.service.v1.AuthMechanism.AUTH_MECHANISM_AUTH0_CLIENT_CREDENTIALS;
import static ai.traceable.ast.hooks.config.service.v1.TestStatus.TEST_STATUS_ABORTED;
import static ai.traceable.ast.hooks.config.service.v1.TestStatus.TEST_STATUS_PENDING;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.ast.hooks.config.service.store.AstHooksConfigStore;
import ai.traceable.ast.hooks.config.service.v1.AdvancedMode;
import ai.traceable.ast.hooks.config.service.v1.AllowedRunners;
import ai.traceable.ast.hooks.config.service.v1.AllowedRunnersInfo;
import ai.traceable.ast.hooks.config.service.v1.AstHookTestDetails;
import ai.traceable.ast.hooks.config.service.v1.AstHookTestFilter;
import ai.traceable.ast.hooks.config.service.v1.CreateAstHookTestRequest;
import ai.traceable.ast.hooks.config.service.v1.DeleteAstHookTestsRequest;
import ai.traceable.ast.hooks.config.service.v1.EnvironmentScope;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookTestResultRequest;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookTestsRequest;
import ai.traceable.ast.hooks.config.service.v1.HookScope;
import ai.traceable.ast.hooks.config.service.v1.Role;
import ai.traceable.ast.hooks.config.service.v1.RunnerInfo;
import ai.traceable.ast.hooks.config.service.v1.TestStatusFilter;
import ai.traceable.ast.hooks.config.service.v1.UpdateAstHookTestRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RequestValidatorTest {
  private static final String TEST_TENANT_ID = "tenant-id";
  private RequestValidator requestValidator;

  @Mock private RequestContext mockRequestContext;
  @Mock private ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  @Mock private ConfigChangeEventGenerator configChangeEventGenerator;

  @BeforeEach
  void setUp() {
    requestValidator =
        new RequestValidator(
            new HookConfigValidator(),
            new AstHooksConfigStore(configServiceBlockingStub, configChangeEventGenerator));
  }

  @Test
  void validateCreateAstHookTestRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, CreateAstHookTestRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "Code snippet not found",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                CreateAstHookTestRequest.newBuilder()
                    .setHookTestDetails(
                        AstHookTestDetails.newBuilder()
                            .setAdvancedMode(
                                AdvancedMode.newBuilder()
                                    .setAuthMechanism(AUTH_MECHANISM_AUTH0_CLIENT_CREDENTIALS)
                                    .build())
                            .build())
                    .build()));

    assertInvalidArgStatusContaining(
        "Allowed runners data not found",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                CreateAstHookTestRequest.newBuilder()
                    .setHookTestDetails(
                        AstHookTestDetails.newBuilder()
                            .setAdvancedMode(
                                AdvancedMode.newBuilder()
                                    .setAuthMechanism(AUTH_MECHANISM_AUTH0_CLIENT_CREDENTIALS)
                                    .setCodeSnippet("code")
                                    .build())
                            .setRole(Role.ROLE_ADMIN)
                            .build())
                    .build()));

    assertInvalidArgStatusContaining(
        "Empty environment found in scope",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                CreateAstHookTestRequest.newBuilder()
                    .setHookTestDetails(
                        AstHookTestDetails.newBuilder()
                            .setAdvancedMode(
                                AdvancedMode.newBuilder()
                                    .setCodeSnippet("codeSnippet")
                                    .setAuthMechanism(AUTH_MECHANISM_AUTH0_CLIENT_CREDENTIALS)
                                    .build())
                            .setRole(Role.ROLE_ADMIN)
                            .setScope(
                                HookScope.newBuilder()
                                    .setEnvironmentScope(
                                        EnvironmentScope.newBuilder()
                                            .addAllEnvironmentIds(List.of(""))
                                            .build())
                                    .build())
                            .build())
                    .setAllowedRunners(
                        AllowedRunners.newBuilder()
                            .setRunnersInfo(
                                AllowedRunnersInfo.newBuilder()
                                    .addRunnersInfo(RunnerInfo.newBuilder().setId("id")))
                            .build())
                    .build()));

    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                CreateAstHookTestRequest.newBuilder()
                    .setHookTestDetails(
                        AstHookTestDetails.newBuilder()
                            .setAdvancedMode(
                                AdvancedMode.newBuilder()
                                    .setCodeSnippet("codeSnippet")
                                    .setAuthMechanism(AUTH_MECHANISM_AUTH0_CLIENT_CREDENTIALS)
                                    .build())
                            .setRole(Role.ROLE_ADMIN)
                            .setScope(
                                HookScope.newBuilder()
                                    .setEnvironmentScope(
                                        EnvironmentScope.newBuilder()
                                            .addAllEnvironmentIds(List.of("env1"))
                                            .build())
                                    .build())
                            .build())
                    .setAllowedRunners(
                        AllowedRunners.newBuilder()
                            .setRunnersInfo(
                                AllowedRunnersInfo.newBuilder()
                                    .addRunnersInfo(RunnerInfo.newBuilder().setId("id")))
                            .build())
                    .build()));
  }

  @Test
  void validateUpdateAstHookTestRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, UpdateAstHookTestRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "UpdateAstHookTestRequest.id",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, UpdateAstHookTestRequest.newBuilder().setLogs("logs").build()));

    assertInvalidArgStatusContaining(
        "Empty Log while trying to update ast hooks test config",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                UpdateAstHookTestRequest.newBuilder().setId("id").setLogs("").build()));

    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                UpdateAstHookTestRequest.newBuilder()
                    .setId("id")
                    .setTestStatus(TEST_STATUS_PENDING)
                    .build()));
  }

  @Test
  void validateGetAstHookTestRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, GetAstHookTestResultRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "GetAstHookTestResultRequest.id",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, GetAstHookTestResultRequest.getDefaultInstance()));

    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, GetAstHookTestResultRequest.newBuilder().setId("id").build()));
  }

  @Test
  void validateGetAstHookTestsRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, GetAstHookTestsRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, GetAstHookTestsRequest.getDefaultInstance()));

    assertInvalidArgStatusContaining(
        "Statuses not found within test status filter",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                GetAstHookTestsRequest.newBuilder()
                    .addAstHookTestFilters(
                        AstHookTestFilter.newBuilder()
                            .setTestStatusFilter(TestStatusFilter.getDefaultInstance())
                            .build())
                    .build()));

    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                GetAstHookTestsRequest.newBuilder()
                    .addAstHookTestFilters(
                        AstHookTestFilter.newBuilder()
                            .setTestStatusFilter(
                                TestStatusFilter.newBuilder()
                                    .addStatuses(TEST_STATUS_ABORTED)
                                    .build())
                            .build())
                    .build()));
  }

  @Test
  void validateDeleteAstHookTestsRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, DeleteAstHookTestsRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "Ids not found while trying to delete ast hook test configs",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, DeleteAstHookTestsRequest.getDefaultInstance()));

    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, DeleteAstHookTestsRequest.newBuilder().addIds("id").build()));
  }

  private void assertInvalidArgStatusContaining(String text, Executable executable) {
    StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, executable);
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    assertTrue(
        Objects.requireNonNull(exception.getStatus().getDescription()).contains(text),
        "Expected arg to contain " + text + " but was " + exception.getMessage());
  }
}
