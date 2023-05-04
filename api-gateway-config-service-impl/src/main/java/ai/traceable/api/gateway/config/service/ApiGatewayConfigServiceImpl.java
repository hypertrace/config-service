package ai.traceable.api.gateway.config.service;

import ai.traceable.api.gateway.config.service.delegate.ApiRoutesCreator;
import ai.traceable.api.gateway.config.service.delegate.ApiRoutesDeleter;
import ai.traceable.api.gateway.config.service.delegate.ApiRoutesGetter;
import ai.traceable.api.gateway.config.service.delegate.ConfigMetadataCreator;
import ai.traceable.api.gateway.config.service.delegate.ConfigMetadataDeleter;
import ai.traceable.api.gateway.config.service.delegate.ConfigMetadataGetter;
import ai.traceable.api.gateway.config.service.v1.ApiGatewayConfigServiceGrpc;
import ai.traceable.api.gateway.config.service.v1.CreateMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.CreateMetadataResponse;
import ai.traceable.api.gateway.config.service.v1.CreateRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.CreateRoutesResponse;
import ai.traceable.api.gateway.config.service.v1.DeleteMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.DeleteMetadataResponse;
import ai.traceable.api.gateway.config.service.v1.DeleteRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.DeleteRoutesResponse;
import ai.traceable.api.gateway.config.service.v1.GetMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.GetMetadataResponse;
import ai.traceable.api.gateway.config.service.v1.GetRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.GetRoutesResponse;
import ai.traceable.api.gateway.config.service.validator.RequestValidator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class ApiGatewayConfigServiceImpl
    extends ApiGatewayConfigServiceGrpc.ApiGatewayConfigServiceImplBase {
  private final RequestValidator requestValidator;

  private final ApiRoutesCreator apiRoutesCreator;
  private final ApiRoutesGetter apiRoutesGetter;
  private final ApiRoutesDeleter apiRoutesDeleter;

  private final ConfigMetadataCreator metadataCreator;
  private final ConfigMetadataGetter metadataGetter;
  private final ConfigMetadataDeleter metadataDeleter;

  @Override
  public void createRoutes(
      final CreateRoutesRequest request,
      final StreamObserver<CreateRoutesResponse> responseObserver) {
    final RequestContext requestContext = RequestContext.CURRENT.get();

    try {
      requestValidator.validate(request, requestContext);
      final CreateRoutesResponse createRoutesResponse =
          apiRoutesCreator.create(request, requestContext);
      responseObserver.onNext(createRoutesResponse);
      responseObserver.onCompleted();
    } catch (final StatusRuntimeException e) {
      log.warn("Unable to create routes ({}) for {}", request, requestContext);
      responseObserver.onError(e);
    } catch (final Exception e) {
      log.warn("Unable to create routes ({}) for {}", request, requestContext);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Error creating routes")
              .asException(requestContext.buildTrailers()));
    }
  }

  @Override
  public void getRoutes(
      final GetRoutesRequest request, final StreamObserver<GetRoutesResponse> responseObserver) {
    final RequestContext requestContext = RequestContext.CURRENT.get();

    try {
      requestValidator.validate(request, requestContext);
      final GetRoutesResponse getRoutesResponse = apiRoutesGetter.get(request, requestContext);
      responseObserver.onNext(getRoutesResponse);
      responseObserver.onCompleted();
    } catch (final StatusRuntimeException e) {
      log.warn("Unable to get routes for request {} and context {}", request, requestContext);
      responseObserver.onError(e);
    } catch (final Exception e) {
      log.warn("Unable to get routes for request {} and context {}", request, requestContext);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Error fetching routes")
              .asException(requestContext.buildTrailers()));
    }
  }

  @Override
  public void deleteRoutes(
      final DeleteRoutesRequest request,
      final StreamObserver<DeleteRoutesResponse> responseObserver) {
    final RequestContext requestContext = RequestContext.CURRENT.get();

    try {
      requestValidator.validate(request, requestContext);
      final DeleteRoutesResponse deleteRoutesResponse =
          apiRoutesDeleter.delete(request, requestContext);
      responseObserver.onNext(deleteRoutesResponse);
      responseObserver.onCompleted();
    } catch (final StatusRuntimeException e) {
      log.warn("Unable to delete routes for request {} and context {}", request, requestContext);
      responseObserver.onError(e);
    } catch (final Exception e) {
      log.warn("Unable to delete routes for request {} and context {}", request, requestContext);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Error deleting routes")
              .asException(requestContext.buildTrailers()));
    }
  }

  @Override
  public void createMetadata(
      final CreateMetadataRequest request,
      final StreamObserver<CreateMetadataResponse> responseObserver) {
    final RequestContext requestContext = RequestContext.CURRENT.get();

    try {
      requestValidator.validate(request, requestContext);
      final CreateMetadataResponse createMetadataResponse =
          metadataCreator.create(request, requestContext);
      responseObserver.onNext(createMetadataResponse);
      responseObserver.onCompleted();
    } catch (final StatusRuntimeException e) {
      log.warn("Unable to create routes ({}) for {}", request, requestContext);
      responseObserver.onError(e);
    } catch (final Exception e) {
      log.warn("Unable to create routes ({}) for {}", request, requestContext);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Error creating routes")
              .asException(requestContext.buildTrailers()));
    }
  }

  @Override
  public void getMetadata(
      final GetMetadataRequest request,
      final StreamObserver<GetMetadataResponse> responseObserver) {
    final RequestContext requestContext = RequestContext.CURRENT.get();

    try {
      requestValidator.validate(request, requestContext);
      final GetMetadataResponse getMetadataResponse = metadataGetter.get(request, requestContext);
      responseObserver.onNext(getMetadataResponse);
      responseObserver.onCompleted();
    } catch (final StatusRuntimeException e) {
      log.warn("Unable to get metadata for request {} and context {}", request, requestContext);
      responseObserver.onError(e);
    } catch (final Exception e) {
      log.warn("Unable to get metadata for request {} and context {}", request, requestContext);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Error fetching metadata")
              .asException(requestContext.buildTrailers()));
    }
  }

  @Override
  public void deleteMetadata(
      final DeleteMetadataRequest request,
      final StreamObserver<DeleteMetadataResponse> responseObserver) {
    final RequestContext requestContext = RequestContext.CURRENT.get();

    try {
      requestValidator.validate(request, requestContext);
      final DeleteMetadataResponse deleteMetadataResponse =
          metadataDeleter.delete(request, requestContext);
      responseObserver.onNext(deleteMetadataResponse);
      responseObserver.onCompleted();
    } catch (final StatusRuntimeException e) {
      log.warn("Unable to delete metadata for request {} and context {}", request, requestContext);
      responseObserver.onError(e);
    } catch (final Exception e) {
      log.warn("Unable to delete metadata for request {} and context {}", request, requestContext);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Error deleting metadata")
              .asException(requestContext.buildTrailers()));
    }
  }
}
