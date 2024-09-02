package ai.traceable.api.spec.config.service;

import static ai.traceable.api.spec.config.service.v1.ApiSpecStatus.API_SPEC_STATUS_UNSPECIFIED;
import static ai.traceable.api.spec.config.service.v1.SpecType.SPEC_TYPE_OPEN_API_SPEC;
import static ai.traceable.api.spec.config.service.v1.SpecType.SPEC_TYPE_UNSPECIFIED;
import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.api.spec.config.service.converter.ApiSpecStatusConverter;
import ai.traceable.api.spec.config.service.store.ApiSpecConfigStore;
import ai.traceable.api.spec.config.service.v1.ApiSpec;
import ai.traceable.api.spec.config.service.v1.ApiSpecConfigServiceGrpc;
import ai.traceable.api.spec.config.service.v1.ApiSpecMetadata;
import ai.traceable.api.spec.config.service.v1.ApiSpecStatus;
import ai.traceable.api.spec.config.service.v1.ApiSpecUpdate;
import ai.traceable.api.spec.config.service.v1.BulkUpdateApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.BulkUpdateApiSpecsResponse;
import ai.traceable.api.spec.config.service.v1.CreateApiSpec;
import ai.traceable.api.spec.config.service.v1.CreateApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.CreateApiSpecResponse;
import ai.traceable.api.spec.config.service.v1.DeleteApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.DeleteApiSpecResponse;
import ai.traceable.api.spec.config.service.v1.DeleteApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.DeleteApiSpecsResponse;
import ai.traceable.api.spec.config.service.v1.GetApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.GetApiSpecResponse;
import ai.traceable.api.spec.config.service.v1.GetApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.GetApiSpecsResponse;
import ai.traceable.api.spec.config.service.v1.OpenApiSpecReference;
import ai.traceable.api.spec.config.service.v1.OpenApiSpecResolutionState;
import ai.traceable.api.spec.config.service.v1.ReferenceApiSpec;
import ai.traceable.api.spec.config.service.v1.ReferenceType;
import ai.traceable.api.spec.config.service.v1.SpecType;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpec;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecResponse;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecsResponse;
import ai.traceable.api.spec.config.service.v1.UpdatedApiSpecField;
import ai.traceable.api.spec.config.service.validation.ApiSpecConfigRequestValidator;
import ai.traceable.config.utils.TimestampConverter;
import com.google.inject.Inject;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ApiSpecConfigServiceImpl
    extends ApiSpecConfigServiceGrpc.ApiSpecConfigServiceImplBase {

  private final TimestampConverter timestampConverter;
  private final ApiSpecConfigRequestValidator validator;
  private final ApiSpecConfigStore apiSpecConfigStore;
  private final ApiSpecConfig apiSpecConfig;
  private final ApiSpecStatusConverter apiSpecStatusConverter;

  @Inject
  public ApiSpecConfigServiceImpl(
      ApiSpecConfigRequestValidator requestValidator,
      ApiSpecConfigStore apiSpecConfigStore,
      TimestampConverter timestampConverter,
      ApiSpecConfig apiSpecConfig,
      ApiSpecStatusConverter apiSpecStatusConverter) {
    this.validator = requestValidator;
    this.apiSpecConfigStore = apiSpecConfigStore;
    this.timestampConverter = timestampConverter;
    this.apiSpecConfig = apiSpecConfig;
    this.apiSpecStatusConverter = apiSpecStatusConverter;
  }

  @Override
  public void getApiSpecs(
      GetApiSpecsRequest request, StreamObserver<GetApiSpecsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      List<ApiSpec> existingApiSpecs = this.apiSpecConfigStore.getAllData(requestContext);
      GetApiSpecsResponse.Builder getApiSpecsResponse = GetApiSpecsResponse.newBuilder();
      if (!request.hasApiSpecFilter()) {
        getApiSpecsResponse.addAllApiSpecs(existingApiSpecs);
      } else {
        List<ApiSpecStatus> convertedStatuses = new ArrayList<>();
        if (request.getApiSpecFilter().hasStatusFilter()) {
          List<ApiSpecStatus> statuses =
              request.getApiSpecFilter().getStatusFilter().getStatusesList();
          convertedStatuses.addAll(
              statuses.stream()
                  .map(this.apiSpecStatusConverter::convert)
                  .collect(toUnmodifiableList()));
        }
        List<ApiSpec> matchedApiSpecs =
            existingApiSpecs.stream()
                // filter based on file sha.
                .filter(
                    spec ->
                        !request.getApiSpecFilter().hasFileContentSha256()
                            || request
                                .getApiSpecFilter()
                                .getFileContentSha256()
                                .getFileContentSha256List()
                                .contains(spec.getFileContentSha256()))
                // filter based on ids
                .filter(
                    spec ->
                        !request.getApiSpecFilter().hasIds()
                            || request
                                .getApiSpecFilter()
                                .getIds()
                                .getValuesList()
                                .contains(spec.getSpecId()))
                // filter based on spec paths
                .filter(
                    spec ->
                        !request.getApiSpecFilter().hasSpecPaths()
                            || request
                                .getApiSpecFilter()
                                .getSpecPaths()
                                .getValuesList()
                                .contains(spec.getSpecPath()))
                // filter based on spec types
                .filter(
                    spec ->
                        !request.getApiSpecFilter().hasSpecTypeFilter()
                            || (request
                                .getApiSpecFilter()
                                .getSpecTypeFilter()
                                .getSpecTypesList()
                                .contains(spec.getSpecType())))
                // filter based on api inspector flag
                .filter(
                    spec ->
                        !request.getApiSpecFilter().hasApiInspectorDisabled()
                            || Boolean.compare(
                                    spec.getApiInspectorDisabled(),
                                    request.getApiSpecFilter().getApiInspectorDisabled())
                                == 0)
                // filter based on references
                .filter(
                    spec ->
                        !request.getApiSpecFilter().hasReferenceApiSpec()
                            || request
                                    .getApiSpecFilter()
                                    .getReferenceApiSpec()
                                    .getReferenceApiSpecsCount()
                                == 0
                            || checkAnyReferenceMatch(request, spec))
                // filter based on reference type
                .filter(
                    spec ->
                        !request.getApiSpecFilter().hasReferenceType()
                            || request
                                    .getApiSpecFilter()
                                    .getReferenceType()
                                    .getReferenceTypesCount()
                                == 0
                            || request
                                .getApiSpecFilter()
                                .getReferenceType()
                                .getReferenceTypesList()
                                .contains(spec.getReferenceType()))
                // filter based on spec names
                .filter(
                    spec ->
                        !request.getApiSpecFilter().hasNames()
                            || request
                                .getApiSpecFilter()
                                .getNames()
                                .getValuesList()
                                .contains(spec.getName()))
                // filter based on statuses
                .filter(
                    spec ->
                        !request.getApiSpecFilter().hasStatusFilter()
                            || convertedStatuses.contains(spec.getStatus()))
                // filter based on spec resolution state
                .filter(
                    spec ->
                        !request.getApiSpecFilter().hasSpecResolutionStateFilter()
                            || checkAnySpecResolutionStateMatch(
                                request
                                    .getApiSpecFilter()
                                    .getSpecResolutionStateFilter()
                                    .getSpecResolutionStatesList(),
                                spec))
                .collect(Collectors.toUnmodifiableList());
        getApiSpecsResponse.addAllApiSpecs(matchedApiSpecs);
      }
      responseObserver.onNext(getApiSpecsResponse.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error getting api specs for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getApiSpec(
      GetApiSpecRequest request, StreamObserver<GetApiSpecResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      ApiSpec apiSpec =
          this.apiSpecConfigStore
              .getData(requestContext, request.getId())
              .orElseThrow(Status.INTERNAL::asRuntimeException);
      // defaulting for backward compatibility
      apiSpec =
          SPEC_TYPE_UNSPECIFIED.equals(apiSpec.getSpecType())
              ? ApiSpec.newBuilder(apiSpec).setSpecType(SpecType.SPEC_TYPE_OPEN_API_SPEC).build()
              : apiSpec;
      responseObserver.onNext(GetApiSpecResponse.newBuilder().setApiSpec(apiSpec).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error getting api spec for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createApiSpec(
      CreateApiSpecRequest request, StreamObserver<CreateApiSpecResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      CreateApiSpec createApiSpec = request.getCreateApiSpec();

      ApiSpec.Builder apiSpecBuilder =
          ApiSpec.newBuilder()
              .setSpecId(UUID.randomUUID().toString())
              .setName(createApiSpec.getName())
              .setApiNamingEnabled(createApiSpec.getApiNamingEnabled())
              .setStatus(this.apiSpecStatusConverter.convert(createApiSpec.getStatus()))
              .setApiDiscoveryEnabled(createApiSpec.getApiDiscoveryEnabled())
              // defaulting for backward compatibility, request side handling due to traceable-cli
              .setSpecType(
                  SPEC_TYPE_UNSPECIFIED.equals(createApiSpec.getSpecType())
                      ? SpecType.SPEC_TYPE_OPEN_API_SPEC
                      : createApiSpec.getSpecType())
              .setApiInspectorDisabled(createApiSpec.getApiInspectorDisabled())
              .setReferenceType(ReferenceType.REFERENCE_TYPE_UNSPECIFIED)
              .setApiSpecMetadata(ApiSpecMetadata.getDefaultInstance());

      List<ApiSpec> existingApiSpecs = this.apiSpecConfigStore.getAllData(requestContext);

      // If optional file hash is supplied in the request.
      if (createApiSpec.hasFileContentSha256()) {
        String inputSha256Hash = createApiSpec.getFileContentSha256();
        boolean sha256Exists =
            existingApiSpecs.stream()
                .map(ApiSpec::getFileContentSha256)
                .anyMatch(inputSha256Hash::equals);
        if (sha256Exists) {
          log.warn(
              "An API spec with the file hash specified in request: {} within context: {} already exists",
              request,
              requestContext);
          throw Status.ALREADY_EXISTS
              .withDescription("An API spec with the same SHA256 file content hash already exists.")
              .asRuntimeException(requestContext.buildTrailers());
        } else {
          apiSpecBuilder.setFileContentSha256(createApiSpec.getFileContentSha256());
        }
      }

      // If spec path already exists then reject the request
      if (createApiSpec.hasSpecPath()) {
        final String specPath = createApiSpec.getSpecPath();
        boolean specPathExists =
            existingApiSpecs.stream().map(ApiSpec::getSpecPath).anyMatch(specPath::equals);
        if (specPathExists) {
          log.warn(
              "An API spec with the spec path specified in request: {} within context: {} already exists",
              request,
              requestContext);
          throw Status.ALREADY_EXISTS
              .withDescription("An API spec with the same spec path already exists.")
              .asRuntimeException(requestContext.buildTrailers());
        } else {
          apiSpecBuilder.setSpecPath(createApiSpec.getSpecPath());
        }
      }

      // Limit number of API specs per tenant.
      if (existingApiSpecs.size() >= apiSpecConfig.getMaxAllowedSpecsPerTenant()) {
        throw Status.RESOURCE_EXHAUSTED
            .withDescription(
                String.format(
                    "Total number of ApiSpec configs created has exceeded the limit for this context: %s",
                    requestContext))
            .asRuntimeException(requestContext.buildTrailers());
      }

      ApiSpec apiSpec = apiSpecBuilder.build();

      ContextualConfigObject<ApiSpec> contextualConfigObject =
          this.apiSpecConfigStore.upsertObject(requestContext, apiSpec);

      responseObserver.onNext(
          CreateApiSpecResponse.newBuilder()
              .setApiSpec(
                  ApiSpec.newBuilder(contextualConfigObject.getData())
                      .setCreationTimestamp(
                          timestampConverter.convert(contextualConfigObject.getCreationTimestamp()))
                      .setLastUpdatedTimestamp(
                          timestampConverter.convert(
                              contextualConfigObject.getLastUpdatedTimestamp()))
                      .build())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error creating api spec for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateApiSpec(
      UpdateApiSpecRequest request, StreamObserver<UpdateApiSpecResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      UpdateApiSpec updateApiSpec = request.getApiSpec();
      ApiSpec existingApiSpec =
          this.apiSpecConfigStore
              .getData(requestContext, updateApiSpec.getSpecId())
              .orElseThrow(Status.NOT_FOUND::asException);
      // defaulting for backward compatibility
      existingApiSpec =
          SPEC_TYPE_UNSPECIFIED.equals(existingApiSpec.getSpecType())
              ? ApiSpec.newBuilder(existingApiSpec)
                  .setSpecType(SpecType.SPEC_TYPE_OPEN_API_SPEC)
                  .build()
              : existingApiSpec;
      ApiSpec updatedApiSpec = buildUpdatedApiSpec(existingApiSpec, updateApiSpec);
      ContextualConfigObject<ApiSpec> contextualConfigObject =
          this.apiSpecConfigStore.upsertObject(requestContext, updatedApiSpec);

      responseObserver.onNext(
          UpdateApiSpecResponse.newBuilder()
              .setApiSpec(buildApiSpec(contextualConfigObject))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating api spec: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateApiSpecs(
      UpdateApiSpecsRequest request, StreamObserver<UpdateApiSpecsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      Map<String, UpdateApiSpec> apiSpecMap =
          request.getApiSpecsList().stream()
              .collect(Collectors.toUnmodifiableMap(UpdateApiSpec::getSpecId, Function.identity()));

      Map<String, ApiSpec> existingApiSpecs =
          apiSpecConfigStore.getAllData(requestContext).stream()
              .filter(apiSpec -> apiSpecMap.containsKey(apiSpec.getSpecId()))
              .collect(Collectors.toUnmodifiableMap(ApiSpec::getSpecId, Function.identity()));
      // check if all the specs corresponding to all the specs in the request exist
      List<String> missingSpecs = new ArrayList<>();
      for (UpdateApiSpec apiSpec : request.getApiSpecsList()) {
        if (!existingApiSpecs.containsKey(apiSpec.getSpecId())) {
          missingSpecs.add(apiSpec.getSpecId());
        }
      }
      if (!missingSpecs.isEmpty()) {
        log.error("Could not find specs with following specIds: " + missingSpecs);
        throw Status.NOT_FOUND.asException();
      }

      List<ApiSpec> updatedApiSpecs = new ArrayList<>();
      for (ApiSpec existingApiSpec : existingApiSpecs.values()) {
        updatedApiSpecs.add(
            buildUpdatedApiSpec(existingApiSpec, apiSpecMap.get(existingApiSpec.getSpecId())));
      }
      List<ContextualConfigObject<ApiSpec>> contextualConfigObjects =
          this.apiSpecConfigStore.upsertObjects(requestContext, updatedApiSpecs);
      updatedApiSpecs =
          contextualConfigObjects.stream().map(this::buildApiSpec).collect(toUnmodifiableList());

      responseObserver.onNext(
          UpdateApiSpecsResponse.newBuilder().addAllApiSpecs(updatedApiSpecs).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating api specs: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void bulkUpdateApiSpecs(
      BulkUpdateApiSpecsRequest request,
      StreamObserver<BulkUpdateApiSpecsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      Map<String, ApiSpecUpdate> specIdToApiSpecUpdateMap =
          request.getApiSpecsList().stream()
              .collect(Collectors.toUnmodifiableMap(ApiSpecUpdate::getSpecId, Function.identity()));

      Map<String, ApiSpec> existingApiSpecs =
          apiSpecConfigStore.getAllData(requestContext).stream()
              .filter(apiSpec -> specIdToApiSpecUpdateMap.containsKey(apiSpec.getSpecId()))
              .collect(Collectors.toUnmodifiableMap(ApiSpec::getSpecId, Function.identity()));
      // check if all the specs corresponding to all the specs in the request exist
      List<String> missingSpecs = new ArrayList<>();
      for (ApiSpecUpdate apiSpec : request.getApiSpecsList()) {
        if (!existingApiSpecs.containsKey(apiSpec.getSpecId())) {
          missingSpecs.add(apiSpec.getSpecId());
        }
      }
      if (!missingSpecs.isEmpty()) {
        log.error("Could not find specs with following specIds: " + missingSpecs);
        throw Status.NOT_FOUND.asException();
      }

      List<ApiSpec> updatedApiSpecs = new ArrayList<>();
      for (ApiSpec existingApiSpec : existingApiSpecs.values()) {
        updatedApiSpecs.add(
            buildPartiallyUpdatedApiSpec(
                existingApiSpec, specIdToApiSpecUpdateMap.get(existingApiSpec.getSpecId())));
      }
      List<ContextualConfigObject<ApiSpec>> contextualConfigObjects =
          this.apiSpecConfigStore.upsertObjects(requestContext, updatedApiSpecs);
      updatedApiSpecs =
          contextualConfigObjects.stream().map(this::buildApiSpec).collect(toUnmodifiableList());

      responseObserver.onNext(
          BulkUpdateApiSpecsResponse.newBuilder().addAllApiSpecs(updatedApiSpecs).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating api specs: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteApiSpec(
      DeleteApiSpecRequest request, StreamObserver<DeleteApiSpecResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      this.apiSpecConfigStore
          .deleteObject(requestContext, request.getSpecId())
          .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));
      log.info("Deleting spec with specId: " + request.getSpecId());

      responseObserver.onNext(DeleteApiSpecResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting api spec: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteApiSpecs(
      DeleteApiSpecsRequest request, StreamObserver<DeleteApiSpecsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      this.apiSpecConfigStore.deleteObjects(requestContext, request.getSpecIdsList());

      responseObserver.onNext(DeleteApiSpecsResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting api specs: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  private ApiSpec buildApiSpec(ContextualConfigObject<ApiSpec> apiSpecContextualConfigObject) {
    return ApiSpec.newBuilder(apiSpecContextualConfigObject.getData())
        .setCreationTimestamp(
            timestampConverter.convert(apiSpecContextualConfigObject.getCreationTimestamp()))
        .setLastUpdatedTimestamp(
            timestampConverter.convert(apiSpecContextualConfigObject.getLastUpdatedTimestamp()))
        .build();
  }

  private ApiSpec buildUpdatedApiSpec(ApiSpec existingApiSpec, UpdateApiSpec updateApiSpec) {
    ApiSpec.Builder apiSpecBuilder =
        ApiSpec.newBuilder(existingApiSpec)
            .setName(updateApiSpec.getName())
            .setApiNamingEnabled(updateApiSpec.getApiNamingEnabled())
            .setApiDiscoveryEnabled(updateApiSpec.getApiDiscoveryEnabled())
            .setApiInspectorDisabled(updateApiSpec.getApiInspectorDisabled())
            .setLastUpdatedTimestamp(Timestamps.now());

    // set updated status if present, else persist with existing status
    if (!API_SPEC_STATUS_UNSPECIFIED.equals(updateApiSpec.getStatus())) {
      apiSpecBuilder.setStatus(this.apiSpecStatusConverter.convert(updateApiSpec.getStatus()));
    }
    return apiSpecBuilder.build();
  }

  private ApiSpec buildPartiallyUpdatedApiSpec(
      ApiSpec existingApiSpec, ApiSpecUpdate apiSpecUpdate) {
    ApiSpec.Builder apiSpecBuilder =
        ApiSpec.newBuilder(existingApiSpec).setLastUpdatedTimestamp(Timestamps.now());
    for (UpdatedApiSpecField updateApiSpecField : apiSpecUpdate.getUpdatedApiSpecFieldsList()) {
      switch (updateApiSpecField.getFieldCase()) {
        case NAME:
          apiSpecBuilder.setName(updateApiSpecField.getName());
          break;
        case API_NAMING_ENABLED:
          apiSpecBuilder.setApiNamingEnabled(updateApiSpecField.getApiNamingEnabled());
          break;
        case STATUS:
          apiSpecBuilder.setStatus(
              this.apiSpecStatusConverter.convert(updateApiSpecField.getStatus()));
          break;
        case API_DISCOVERY_ENABLED:
          apiSpecBuilder.setApiDiscoveryEnabled(updateApiSpecField.getApiDiscoveryEnabled());
          break;
        case API_INSPECTOR_DISABLED:
          apiSpecBuilder.setApiInspectorDisabled(updateApiSpecField.getApiInspectorDisabled());
          break;
        case REFERENCE_TYPE:
          apiSpecBuilder.setReferenceType(updateApiSpecField.getReferenceType());
          break;
        case API_SPEC_METADATA:
          apiSpecBuilder.setApiSpecMetadata(updateApiSpecField.getApiSpecMetadata());
          break;
        case SPEC_PATH:
          apiSpecBuilder.setSpecPath(updateApiSpecField.getSpecPath());
          break;
        case FIELD_NOT_SET:
        default:
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  String.format(
                      "Invalid request. At least 1 update field is to be set in request to spec id: %s",
                      apiSpecUpdate.getSpecId()))
              .asRuntimeException();
      }
    }
    return apiSpecBuilder.build();
  }

  private boolean checkAnyReferenceMatch(GetApiSpecsRequest request, ApiSpec spec) {
    return spec.hasApiSpecMetadata()
        && spec.getApiSpecMetadata().hasOpenApiSpecMetadata()
        && spec
            .getApiSpecMetadata()
            .getOpenApiSpecMetadata()
            .getOpenApiSpecReferencesList()
            .stream()
            .anyMatch(specMetaDataReference -> checkAnyFilterMatch(request, specMetaDataReference));
  }

  private boolean checkAnyFilterMatch(
      GetApiSpecsRequest request, OpenApiSpecReference specMetaDataReference) {
    return request.getApiSpecFilter().getReferenceApiSpec().getReferenceApiSpecsList().stream()
        .anyMatch(
            referenceSpec ->
                checkReferenceSpecIdOrSpecPathMatch(specMetaDataReference, referenceSpec));
  }

  private boolean checkReferenceSpecIdOrSpecPathMatch(
      OpenApiSpecReference specMetaDataReference, ReferenceApiSpec referenceSpec) {
    if (referenceSpec.hasSpecId()) {
      if (specMetaDataReference.hasCompleteOpenApiSpecReference()) {
        return referenceSpec
            .getSpecId()
            .equals(specMetaDataReference.getCompleteOpenApiSpecReference().getSpecId());
      } else if (specMetaDataReference.hasIncompleteOpenApiSpecReference()) {
        return referenceSpec
            .getSpecId()
            .equals(specMetaDataReference.getIncompleteOpenApiSpecReference().getSpecId());
      }
    } else if (referenceSpec.hasSpecPath()) {
      return referenceSpec.getSpecPath().equals(specMetaDataReference.getResolvedSpecPath());
    }
    return false;
  }

  private boolean checkAnySpecResolutionStateMatch(
      List<OpenApiSpecResolutionState> apiSpecResolutionStates, ApiSpec spec) {
    if (!SPEC_TYPE_OPEN_API_SPEC.equals(spec.getSpecType())) {
      return false;
    }

    if (!spec.hasApiSpecMetadata() || !spec.getApiSpecMetadata().hasOpenApiSpecMetadata()) {
      return false;
    }

    return apiSpecResolutionStates.contains(
        spec.getApiSpecMetadata().getOpenApiSpecMetadata().getOpenApiSpecResolutionState());
  }
}
