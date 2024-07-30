package ai.traceable.api.spec.config.service.validation;

import static ai.traceable.api.spec.config.service.v1.ApiSpecStatus.API_SPEC_STATUS_COMPLETED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.api.spec.config.service.v1.ApiSpecMetadata;
import ai.traceable.api.spec.config.service.v1.ApiSpecUpdate;
import ai.traceable.api.spec.config.service.v1.BulkUpdateApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.CompleteOpenApiSpecReference;
import ai.traceable.api.spec.config.service.v1.CreateApiSpec;
import ai.traceable.api.spec.config.service.v1.CreateApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.DeleteApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.DeleteApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.GetApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.GetApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.IncompleteOpenApiSpecReference;
import ai.traceable.api.spec.config.service.v1.MissingOpenApiSpecReference;
import ai.traceable.api.spec.config.service.v1.OpenApiSpecMetadata;
import ai.traceable.api.spec.config.service.v1.OpenApiSpecReference;
import ai.traceable.api.spec.config.service.v1.ReferenceType;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpec;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.UpdatedApiSpecField;
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
                    .setCreateApiSpec(CreateApiSpec.newBuilder())
                    .build()));

    assertInvalidArgStatusContaining(
        "CreateApiSpec.status",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(CreateApiSpec.newBuilder().setName("name"))
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
                            .setSpecPath("/test/name.json"))
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
                    .setApiSpec(UpdateApiSpec.newBuilder().setName("name"))
                    .build()));

    assertInvalidArgStatusContaining(
        "UpdateApiSpec.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiSpecRequest.newBuilder()
                    .setApiSpec(UpdateApiSpec.newBuilder().setSpecId("id"))
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
                            .setStatus(API_SPEC_STATUS_COMPLETED))
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
                    .addApiSpecs(UpdateApiSpec.newBuilder().setName("name"))
                    .build()));

    assertInvalidArgStatusContaining(
        "UpdateApiSpec.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiSpecsRequest.newBuilder()
                    .addApiSpecs(UpdateApiSpec.newBuilder().setSpecId("id"))
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
                            .setStatus(API_SPEC_STATUS_COMPLETED))
                    .build()));
  }

  @Test
  void validatesApiSpecsBulkUpdateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, BulkUpdateApiSpecsRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "Invalid request. At least 1 spec is to be provided in request",
        () ->
            validator.validateOrThrow(
                mockRequestContext, BulkUpdateApiSpecsRequest.newBuilder().build()));

    assertInvalidArgStatusContaining(
        "ApiSpecUpdate.spec_id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                BulkUpdateApiSpecsRequest.newBuilder()
                    .addAllApiSpecs(
                        List.of(
                            ApiSpecUpdate.newBuilder()
                                .addAllUpdatedApiSpecFields(
                                    List.of(
                                        UpdatedApiSpecField.newBuilder().setName("name").build()))
                                .build()))
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid request. At least 1 update field is to be provided in request to spec id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                BulkUpdateApiSpecsRequest.newBuilder()
                    .addAllApiSpecs(List.of(ApiSpecUpdate.newBuilder().setSpecId("id").build()))
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid request. At least 1 update field is to be set in request to spec id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                BulkUpdateApiSpecsRequest.newBuilder()
                    .addAllApiSpecs(
                        List.of(
                            ApiSpecUpdate.newBuilder()
                                .setSpecId("id")
                                .addAllUpdatedApiSpecFields(
                                    List.of(UpdatedApiSpecField.newBuilder().build()))
                                .build()))
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid request. Field is updated more than once for spec with id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                BulkUpdateApiSpecsRequest.newBuilder()
                    .addAllApiSpecs(
                        List.of(
                            ApiSpecUpdate.newBuilder()
                                .setSpecId("id")
                                .addAllUpdatedApiSpecFields(
                                    List.of(
                                        UpdatedApiSpecField.newBuilder().setName("name1").build(),
                                        UpdatedApiSpecField.newBuilder().setName("name2").build()))
                                .build()))
                    .build()));

    assertInvalidArgStatusContaining(
        "UpdatedApiSpecField.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                BulkUpdateApiSpecsRequest.newBuilder()
                    .addAllApiSpecs(
                        List.of(
                            ApiSpecUpdate.newBuilder()
                                .setSpecId("id")
                                .addAllUpdatedApiSpecFields(
                                    List.of(UpdatedApiSpecField.newBuilder().setName("").build()))
                                .build()))
                    .build()));

    assertInvalidArgStatusContaining(
        "OpenApiSpecReference.resolved_spec_path",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                BulkUpdateApiSpecsRequest.newBuilder()
                    .addAllApiSpecs(
                        List.of(
                            ApiSpecUpdate.newBuilder()
                                .setSpecId("id")
                                .addAllUpdatedApiSpecFields(
                                    List.of(
                                        UpdatedApiSpecField.newBuilder()
                                            .setApiSpecMetadata(
                                                ApiSpecMetadata.newBuilder()
                                                    .setOpenApiSpecMetadata(
                                                        OpenApiSpecMetadata.newBuilder()
                                                            .addAllOpenApiSpecReferences(
                                                                List.of(
                                                                    OpenApiSpecReference
                                                                        .newBuilder()
                                                                        .setMissingOpenApiSpecReference(
                                                                            MissingOpenApiSpecReference
                                                                                .newBuilder())
                                                                        .build()))))
                                            .build()))
                                .build()))
                    .build()));

    assertInvalidArgStatusContaining(
        "IncompleteOpenApiSpecReference.spec_id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                BulkUpdateApiSpecsRequest.newBuilder()
                    .addAllApiSpecs(
                        List.of(
                            ApiSpecUpdate.newBuilder()
                                .setSpecId("id")
                                .addAllUpdatedApiSpecFields(
                                    List.of(
                                        UpdatedApiSpecField.newBuilder()
                                            .setApiSpecMetadata(
                                                ApiSpecMetadata.newBuilder()
                                                    .setOpenApiSpecMetadata(
                                                        OpenApiSpecMetadata.newBuilder()
                                                            .addAllOpenApiSpecReferences(
                                                                List.of(
                                                                    OpenApiSpecReference
                                                                        .newBuilder()
                                                                        .setResolvedSpecPath(
                                                                            "/test/spec2.json")
                                                                        .setIncompleteOpenApiSpecReference(
                                                                            IncompleteOpenApiSpecReference
                                                                                .newBuilder())
                                                                        .build()))))
                                            .build()))
                                .build()))
                    .build()));

    assertInvalidArgStatusContaining(
        "CompleteOpenApiSpecReference.spec_id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                BulkUpdateApiSpecsRequest.newBuilder()
                    .addAllApiSpecs(
                        List.of(
                            ApiSpecUpdate.newBuilder()
                                .setSpecId("id")
                                .addAllUpdatedApiSpecFields(
                                    List.of(
                                        UpdatedApiSpecField.newBuilder()
                                            .setApiSpecMetadata(
                                                ApiSpecMetadata.newBuilder()
                                                    .setOpenApiSpecMetadata(
                                                        OpenApiSpecMetadata.newBuilder()
                                                            .addAllOpenApiSpecReferences(
                                                                List.of(
                                                                    OpenApiSpecReference
                                                                        .newBuilder()
                                                                        .setResolvedSpecPath(
                                                                            "/test/spec2.json")
                                                                        .setCompleteOpenApiSpecReference(
                                                                            CompleteOpenApiSpecReference
                                                                                .newBuilder())
                                                                        .build()))))
                                            .build()))
                                .build()))
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid request. Repeated update to spec detected in request",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                BulkUpdateApiSpecsRequest.newBuilder()
                    .addAllApiSpecs(
                        List.of(
                            ApiSpecUpdate.newBuilder()
                                .setSpecId("id1")
                                .addAllUpdatedApiSpecFields(
                                    List.of(
                                        UpdatedApiSpecField.newBuilder().setName("name1").build()))
                                .build(),
                            ApiSpecUpdate.newBuilder()
                                .setSpecId("id1")
                                .addAllUpdatedApiSpecFields(
                                    List.of(
                                        UpdatedApiSpecField.newBuilder().setName("name2").build()))
                                .build()))
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                BulkUpdateApiSpecsRequest.newBuilder()
                    .addAllApiSpecs(
                        List.of(
                            ApiSpecUpdate.newBuilder()
                                .setSpecId("id1")
                                .addAllUpdatedApiSpecFields(
                                    List.of(
                                        UpdatedApiSpecField.newBuilder().setName("name1").build(),
                                        UpdatedApiSpecField.newBuilder()
                                            .setApiNamingEnabled(true)
                                            .build()))
                                .build(),
                            ApiSpecUpdate.newBuilder()
                                .setSpecId("id2")
                                .addAllUpdatedApiSpecFields(
                                    List.of(
                                        UpdatedApiSpecField.newBuilder()
                                            .setReferenceType(
                                                ReferenceType.REFERENCE_TYPE_REFERENCE)
                                            .build(),
                                        UpdatedApiSpecField.newBuilder()
                                            .setApiSpecMetadata(
                                                ApiSpecMetadata.newBuilder()
                                                    .setOpenApiSpecMetadata(
                                                        OpenApiSpecMetadata.newBuilder()
                                                            .addAllOpenApiSpecReferences(
                                                                List.of(
                                                                    OpenApiSpecReference
                                                                        .newBuilder()
                                                                        .setResolvedSpecPath(
                                                                            "/test/spec2.json")
                                                                        .setCompleteOpenApiSpecReference(
                                                                            CompleteOpenApiSpecReference
                                                                                .newBuilder()
                                                                                .setSpecId("id2"))
                                                                        .build()))))
                                            .build()))
                                .build()))
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
