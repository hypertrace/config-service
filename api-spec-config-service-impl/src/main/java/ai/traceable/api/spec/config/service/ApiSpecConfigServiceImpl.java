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
import ai.traceable.api.spec.config.service.v1.GetApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.GetApiSpecResponse;
import ai.traceable.api.spec.config.service.v1.GetApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.GetApiSpecsResponse;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpec;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecResponse;
import ai.traceable.api.spec.config.service.validation.ApiSpecConfigRequestValidator;
import ai.traceable.config.utils.TimestampConverter;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ApiSpecConfigServiceImpl
    extends ApiSpecConfigServiceGrpc.ApiSpecConfigServiceImplBase {

  private final TimestampConverter timestampConverter;
  private final ApiSpecConfigRequestValidator validator;
  private final ApiSpecConfigStore apiSpecConfigStore;

  @Inject
  public ApiSpecConfigServiceImpl(
      ApiSpecConfigRequestValidator requestValidator,
      ApiSpecConfigStore apiSpecConfigStore,
      TimestampConverter timestampConverter) {
    this.validator = requestValidator;
    this.apiSpecConfigStore = apiSpecConfigStore;
    this.timestampConverter = timestampConverter;
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

      ApiSpec apiSpec =
          ApiSpec.newBuilder()
              .setSpecId(UUID.randomUUID().toString())
              .setName(createApiSpec.getName())
              .setApiNamingEnabled(createApiSpec.getApiNamingEnabled())
              .setStatus(createApiSpec.getStatus())
              .build();

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
    } catch (Exception exception) {
      log.error("Error updating api spec: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteApiSpec(
      DeleteApiSpecRequest request, StreamObserver<DeleteApiSpecResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      this.apiSpecConfigStore.deleteObject(requestContext, request.getSpecId());

      responseObserver.onNext(DeleteApiSpecResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting api spec: {}", request, exception);
      responseObserver.onError(exception);
    }
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
