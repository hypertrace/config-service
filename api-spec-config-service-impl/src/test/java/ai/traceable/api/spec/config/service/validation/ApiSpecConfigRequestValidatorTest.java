package ai.traceable.api.spec.config.service.validation;

import static ai.traceable.api.spec.config.service.v1.ApiSpecStatus.API_SPEC_STATUS_COMPLETED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.api.spec.config.service.v1.CreateApiSpec;
import ai.traceable.api.spec.config.service.v1.CreateApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.DeleteApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.DeleteApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.GetApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.GetApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpec;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecsRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ApiSpecConfigRequestValidatorTest {

  private static final String TEST_TENANT_ID = "ApiSpecConfigRequestValidatorTest-tenant-id";
  private final ApiSpecConfigRequestValidator validator = new ApiSpecConfigRequestValidator();

  @Mock private RequestContext mockRequestContext;

  @Test
  void validatesApiSpecsGetRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(mockRequestContext, GetApiSpecsRequest.newBuilder().build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(mockRequestContext, GetApiSpecsRequest.newBuilder().build()));
  }

  @Test
  void validatesApiSpecGetRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(mockRequestContext, GetApiSpecRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "GetApiSpecRequest.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetApiSpecRequest.newBuilder().setId("").build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetApiSpecRequest.newBuilder().setId("id").build()));
  }

  @Test
  void validatesApiSpecCreateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateApiSpecRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "CreateApiSpec.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(CreateApiSpec.newBuilder().build())
                    .build()));

    assertInvalidArgStatusContaining(
        "CreateApiSpec.status",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(CreateApiSpec.newBuilder().setName("name").build())
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("name")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_COMPLETED)
                            .setSpecPath("/test/name.json")
                            .build())
                    .build()));
  }

  @Test
  void validatesApiSpecUpdateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateApiSpecRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "UpdateApiSpec.spec_id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiSpecRequest.newBuilder()
                    .setApiSpec(UpdateApiSpec.newBuilder().setName("name").build())
                    .build()));

    assertInvalidArgStatusContaining(
        "UpdateApiSpec.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiSpecRequest.newBuilder()
                    .setApiSpec(UpdateApiSpec.newBuilder().setSpecId("id").build())
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiSpecRequest.newBuilder()
                    .setApiSpec(
                        UpdateApiSpec.newBuilder()
                            .setSpecId("id")
                            .setName("name")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_COMPLETED)
                            .build())
                    .build()));
  }

  @Test
  void validatesApiSpecsUpdateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateApiSpecsRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "UpdateApiSpec.spec_id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiSpecsRequest.newBuilder()
                    .addApiSpecs(UpdateApiSpec.newBuilder().setName("name").build())
                    .build()));

    assertInvalidArgStatusContaining(
        "UpdateApiSpec.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiSpecsRequest.newBuilder()
                    .addApiSpecs(UpdateApiSpec.newBuilder().setSpecId("id").build())
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid request. At least 1 spec is to be provided in request",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateApiSpecsRequest.newBuilder().build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiSpecsRequest.newBuilder()
                    .addApiSpecs(
                        UpdateApiSpec.newBuilder()
                            .setSpecId("id")
                            .setName("name")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_COMPLETED)
                            .build())
                    .build()));
  }

  @Test
  void validatesApiSpecDeleteRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteApiSpecRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "DeleteApiSpecRequest.spec_id",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteApiSpecRequest.newBuilder().setSpecId("").build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteApiSpecRequest.newBuilder().setSpecId("id").build()));
  }

  @Test
  void validatesApiSpecsDeleteRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteApiSpecsRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "Invalid request. At least 1 spec id is to be provided in request",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteApiSpecsRequest.newBuilder().build()));

    assertInvalidArgStatusContaining(
        "Invalid specId in request",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                DeleteApiSpecsRequest.newBuilder().addAllSpecIds(List.of("", "id")).build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                DeleteApiSpecsRequest.newBuilder().addAllSpecIds(List.of("spec-id")).build()));
  }

  private void assertInvalidArgStatusContaining(String text, Executable executable) {
    StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, executable);
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    assertTrue(
        Objects.requireNonNull(exception.getStatus().getDescription()).contains(text),
        "Expected arg to contain " + text + " but was " + exception.getMessage());
  }
}
