package ai.traceable.api.spec.config.service.validation;

import static ai.traceable.api.spec.config.service.v1.ApiSpecMetadata.SpecMetadataTypeCase.OPEN_API_SPEC_METADATA;
import static ai.traceable.api.spec.config.service.v1.UpdatedApiSpecField.FieldCase.FIELD_NOT_SET;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

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
import ai.traceable.api.spec.config.service.v1.OpenApiSpecMetadata;
import ai.traceable.api.spec.config.service.v1.OpenApiSpecReference;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpec;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.UpdatedApiSpecField;
import ai.traceable.api.spec.config.service.v1.UpdatedApiSpecField.FieldCase;
import io.grpc.Status;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ApiSpecConfigRequestValidator {

  private static final Pattern SHA256_PATTERN = Pattern.compile("^[a-fA-F0-9]{64}$");

  public void validateOrThrow(RequestContext requestContext, GetApiSpecsRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(RequestContext requestContext, GetApiSpecRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, GetApiSpecRequest.ID_FIELD_NUMBER);
  }

  public void validateOrThrow(RequestContext requestContext, CreateApiSpecRequest request) {
    validateRequestContextOrThrow(requestContext);
    CreateApiSpec createApiSpec = request.getCreateApiSpec();
    validateNonDefaultPresenceOrThrow(createApiSpec, CreateApiSpec.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(createApiSpec, CreateApiSpec.STATUS_FIELD_NUMBER);
    if (createApiSpec.hasFileContentSha256()) {
      if (!SHA256_PATTERN.matcher(createApiSpec.getFileContentSha256()).matches()) {
        throw Status.INVALID_ARGUMENT
            .withDescription("File Content Hash is not SHA256")
            .asRuntimeException(requestContext.buildTrailers());
      }
    }
  }

  public void validateOrThrow(RequestContext requestContext, UpdateApiSpecRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateUpdateApiSpec(request.getApiSpec());
  }

  public void validateOrThrow(RequestContext requestContext, UpdateApiSpecsRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request.getApiSpecsCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid request. At least 1 spec is to be provided in request: %s", request))
          .asRuntimeException();
    }
    for (UpdateApiSpec updateApiSpec : request.getApiSpecsList()) {
      this.validateUpdateApiSpec(updateApiSpec);
    }
  }

  public void validateOrThrow(RequestContext requestContext, BulkUpdateApiSpecsRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request.getApiSpecsCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid request. At least 1 spec is to be provided in request: %s", request))
          .asRuntimeException();
    }
    Set<String> updateApiSpecIds = new HashSet<>();
    for (ApiSpecUpdate apiSpecUpdate : request.getApiSpecsList()) {
      this.validateApiSpecUpdate(apiSpecUpdate);
      if (updateApiSpecIds.contains(apiSpecUpdate.getSpecId())) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid request. Repeated update to spec detected in request: %s", request))
            .asRuntimeException();
      }
      updateApiSpecIds.add(apiSpecUpdate.getSpecId());
    }
  }

  public void validateOrThrow(RequestContext requestContext, DeleteApiSpecRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteApiSpecRequest.SPEC_ID_FIELD_NUMBER);
  }

  public void validateOrThrow(RequestContext requestContext, DeleteApiSpecsRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request.getSpecIdsCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid request. At least 1 spec id is to be provided in request: %s", request))
          .asRuntimeException();
    }
    for (String id : request.getSpecIdsList()) {
      if (id.isEmpty() || id.isBlank()) {
        throw Status.INVALID_ARGUMENT
            .withDescription(String.format("Invalid specId in request: %s", id))
            .asRuntimeException();
      }
    }
  }

  private void validateUpdateApiSpec(UpdateApiSpec updateApiSpec) {
    validateNonDefaultPresenceOrThrow(updateApiSpec, UpdateApiSpec.SPEC_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(updateApiSpec, UpdateApiSpec.NAME_FIELD_NUMBER);
  }

  private void validateApiSpecUpdate(ApiSpecUpdate apiSpecUpdate) {
    validateNonDefaultPresenceOrThrow(apiSpecUpdate, ApiSpecUpdate.SPEC_ID_FIELD_NUMBER);
    if (apiSpecUpdate.getUpdatedApiSpecFieldsCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid request. At least 1 update field is to be provided in request to spec id: %s",
                  apiSpecUpdate.getSpecId()))
          .asRuntimeException();
    }

    Set<FieldCase> updatedFieldCases = new HashSet<>();
    for (UpdatedApiSpecField updatedApiSpecField : apiSpecUpdate.getUpdatedApiSpecFieldsList()) {
      // Ensure that a field is set
      if (FIELD_NOT_SET.equals(updatedApiSpecField.getFieldCase())) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid request. At least 1 update field is to be set in request to spec id: %s",
                    apiSpecUpdate.getSpecId()))
            .asRuntimeException();
      }

      // Check duplicate updates
      if (updatedFieldCases.contains(updatedApiSpecField.getFieldCase())) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid request. Field is updated more than once for spec with id: %s",
                    apiSpecUpdate.getSpecId()))
            .asRuntimeException();
      }
      updatedFieldCases.add(updatedApiSpecField.getFieldCase());

      this.validateApiSpecUpdateIndividualField(updatedApiSpecField);
    }
  }

  private void validateApiSpecUpdateIndividualField(UpdatedApiSpecField updatedApiSpecField) {
    if (updatedApiSpecField.hasName()) {
      validateNonDefaultPresenceOrThrow(updatedApiSpecField, UpdatedApiSpecField.NAME_FIELD_NUMBER);
    }
    if (updatedApiSpecField.hasReferenceType()) {
      validateNonDefaultPresenceOrThrow(
          updatedApiSpecField, UpdatedApiSpecField.REFERENCE_TYPE_FIELD_NUMBER);
    }
    if (updatedApiSpecField.hasApiSpecMetadata()) {
      if (OPEN_API_SPEC_METADATA.equals(
          updatedApiSpecField.getApiSpecMetadata().getSpecMetadataTypeCase())) {
        this.validateOpenApiSpecMetadata(
            updatedApiSpecField.getApiSpecMetadata().getOpenApiSpecMetadata());
      }
    }
    if (updatedApiSpecField.hasSpecPath()) {
      validateNonDefaultPresenceOrThrow(
          updatedApiSpecField, UpdatedApiSpecField.SPEC_PATH_FIELD_NUMBER);
    }
  }

  private void validateOpenApiSpecMetadata(OpenApiSpecMetadata openApiSpecMetadata) {
    for (OpenApiSpecReference openApiSpecReference :
        openApiSpecMetadata.getOpenApiSpecReferencesList()) {
      validateNonDefaultPresenceOrThrow(
          openApiSpecReference, OpenApiSpecReference.RESOLVED_SPEC_PATH_FIELD_NUMBER);

      switch (openApiSpecReference.getResolutionResultCase()) {
        case MISSING_OPEN_API_SPEC_REFERENCE:
          break;
        case INCOMPLETE_OPEN_API_SPEC_REFERENCE:
          validateNonDefaultPresenceOrThrow(
              openApiSpecReference.getIncompleteOpenApiSpecReference(),
              IncompleteOpenApiSpecReference.SPEC_ID_FIELD_NUMBER);
          break;
        case COMPLETE_OPEN_API_SPEC_REFERENCE:
          validateNonDefaultPresenceOrThrow(
              openApiSpecReference.getCompleteOpenApiSpecReference(),
              CompleteOpenApiSpecReference.SPEC_ID_FIELD_NUMBER);
          break;
        case RESOLUTIONRESULT_NOT_SET:
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  String.format(
                      "Resolution result type not set for reference with path: %s",
                      openApiSpecReference.getResolvedSpecPath()))
              .asRuntimeException();
      }
    }
  }
}
