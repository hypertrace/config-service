package ai.traceable.api.spec.config.service;

import static ai.traceable.api.spec.config.service.v1.ApiSpecStatus.API_SPEC_STATUS_UNSPECIFIED;

import ai.traceable.api.spec.config.service.store.ApiSpecConfigStore;
import ai.traceable.api.spec.config.service.v1.ApiSpec;
import ai.traceable.api.spec.config.service.v1.ApiSpecConfigServiceGrpc;
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
import ai.traceable.api.spec.config.service.v1.UpdateApiSpec;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecResponse;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecsResponse;
import ai.traceable.api.spec.config.service.validation.ApiSpecConfigRequestValidator;
import ai.traceable.config.utils.TimestampConverter;
import com.google.inject.Inject;
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

  @Inject
  public ApiSpecConfigServiceImpl(
      ApiSpecConfigRequestValidator requestValidator,
      ApiSpecConfigStore apiSpecConfigStore,
      TimestampConverter timestampConverter,
      ApiSpecConfig apiSpecConfig) {
    this.validator = requestValidator;
    this.apiSpecConfigStore = apiSpecConfigStore;
    this.timestampConverter = timestampConverter;
    this.apiSpecConfig = apiSpecConfig;
  }

  @Override
  public void getApiSpecs(
      GetApiSpecsRequest request, StreamObserver<GetApiSpecsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          GetApiSpecsResponse.newBuilder()
              .addAllApiSpecs(this.apiSpecConfigStore.getAllData(requestContext))
              .build());
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

      responseObserver.onNext(
          GetApiSpecResponse.newBuilder()
              .setApiSpec(
                  this.apiSpecConfigStore
                      .getData(requestContext, request.getId())
                      .orElseThrow(Status.INTERNAL::asRuntimeException))
              .build());
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
              .setStatus(createApiSpec.getStatus());

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
          contextualConfigObjects.stream()
              .map(this::buildApiSpec)
              .collect(Collectors.toUnmodifiableList());

      responseObserver.onNext(
          UpdateApiSpecsResponse.newBuilder().addAllApiSpecs(updatedApiSpecs).build());
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
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
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

      for (String id : request.getSpecIdsList()) {
        this.apiSpecConfigStore.deleteObject(requestContext, id);
        log.info("Deleting spec with specId: " + id);
      }
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
            .setApiNamingEnabled(updateApiSpec.getApiNamingEnabled());

    // set updated status if present, else persist with existing status
    if (!API_SPEC_STATUS_UNSPECIFIED.equals(updateApiSpec.getStatus())) {
      apiSpecBuilder.setStatus(updateApiSpec.getStatus());
    }
    return apiSpecBuilder.build();
  }
}
