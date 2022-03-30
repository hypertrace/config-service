package ai.traceable.api.attribute.override.service;

import ai.traceable.api.attribute.override.service.handlers.GetAllApiAttributeOverridesHandler;
import ai.traceable.api.attribute.override.service.handlers.RemoveApiAttributeOverridesHandler;
import ai.traceable.api.attribute.override.service.handlers.UpsertApiAttributeOverridesHandler;
import ai.traceable.api.attribute.override.service.v1.ApiAttributeOverrideServiceGrpc.ApiAttributeOverrideServiceImplBase;
import ai.traceable.api.attribute.override.service.v1.ApiAttributeOverrides;
import ai.traceable.api.attribute.override.service.v1.GetAllApiAttributeOverridesRequest;
import ai.traceable.api.attribute.override.service.v1.GetAllApiAttributeOverridesResponse;
import ai.traceable.api.attribute.override.service.v1.RemoveApiAttributeOverridesRequest;
import ai.traceable.api.attribute.override.service.v1.RemoveApiAttributeOverridesResponse;
import ai.traceable.api.attribute.override.service.v1.UpsertApiAttributeOverridesRequest;
import ai.traceable.api.attribute.override.service.v1.UpsertApiAttributeOverridesResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ApiAttributeOverrideServiceImpl extends ApiAttributeOverrideServiceImplBase {
  private final ApiAttributeOverrideRequestValidator requestValidator;
  private final GetAllApiAttributeOverridesHandler getAllApiAttributeOverridesHandler;
  private final UpsertApiAttributeOverridesHandler upsertApiAttributeOverridesHandler;
  private final RemoveApiAttributeOverridesHandler removeApiAttributeOverridesHandler;

  @Inject
  public ApiAttributeOverrideServiceImpl(
      ApiAttributeOverrideRequestValidator requestValidator,
      GetAllApiAttributeOverridesHandler getAllApiAttributeOverridesHandler,
      UpsertApiAttributeOverridesHandler upsertApiAttributeOverridesHandler,
      RemoveApiAttributeOverridesHandler removeApiAttributeOverridesHandler) {
    this.requestValidator = requestValidator;
    this.getAllApiAttributeOverridesHandler = getAllApiAttributeOverridesHandler;
    this.upsertApiAttributeOverridesHandler = upsertApiAttributeOverridesHandler;
    this.removeApiAttributeOverridesHandler = removeApiAttributeOverridesHandler;
  }

  @Override
  public void getAllApiAttributeOverrides(
      GetAllApiAttributeOverridesRequest request,
      StreamObserver<GetAllApiAttributeOverridesResponse> responseObserver) {
    Status status = requestValidator.validate(request);
    if (!status.isOk()) {
      log.error("Unable to get all api attribute overrides, invalid request {}", request);
      responseObserver.onError(status.asException());
      return;
    }
    try {
      Map<String, ApiAttributeOverrides> apiAttributeOverridesMap =
          getAllApiAttributeOverridesHandler.getOverrides(request, RequestContext.CURRENT.get());
      GetAllApiAttributeOverridesResponse response =
          GetAllApiAttributeOverridesResponse.newBuilder()
              .putAllApiAttributeOverrides(apiAttributeOverridesMap)
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error getting all api attribute overrides {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void upsertApiAttributeOverrides(
      UpsertApiAttributeOverridesRequest request,
      StreamObserver<UpsertApiAttributeOverridesResponse> responseObserver) {
    Status status = requestValidator.validate(request);
    if (!status.isOk()) {
      log.error("Unable to upsert api attribute overrides, invalid request {}", request);
      responseObserver.onError(status.asException());
      return;
    }
    try {
      ApiAttributeOverrides upsertedAttributeOverride =
          upsertApiAttributeOverridesHandler.upsertAttributeOverrides(
              RequestContext.CURRENT.get(), request);
      UpsertApiAttributeOverridesResponse response =
          UpsertApiAttributeOverridesResponse.newBuilder()
              .setApiAttributeOverrides(upsertedAttributeOverride)
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error upserting api attribute overrides {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void removeApiAttributeOverrides(
      RemoveApiAttributeOverridesRequest request,
      StreamObserver<RemoveApiAttributeOverridesResponse> responseObserver) {
    Status status = requestValidator.validate(request);
    if (!status.isOk()) {
      log.error("Unable to remove api attribute overrides, invalid request {}", request);
      responseObserver.onError(status.asException());
      return;
    }
    try {
      removeApiAttributeOverridesHandler.removeAttributeOverrides(
          RequestContext.CURRENT.get(), request);
      responseObserver.onNext(RemoveApiAttributeOverridesResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error removing api attribute overrides {}", request, e);
      responseObserver.onError(e);
    }
  }
}
