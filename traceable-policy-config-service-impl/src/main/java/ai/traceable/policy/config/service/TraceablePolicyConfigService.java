package ai.traceable.policy.config.service;

import ai.traceable.policy.config.service.store.TraceablePolicyConfigStoreManager;
import ai.traceable.policy.config.service.v1.DeleteRequest;
import ai.traceable.policy.config.service.v1.DeleteResponse;
import ai.traceable.policy.config.service.v1.GetAllRequest;
import ai.traceable.policy.config.service.v1.GetAllResponse;
import ai.traceable.policy.config.service.v1.GetRequest;
import ai.traceable.policy.config.service.v1.GetResponse;
import ai.traceable.policy.config.service.v1.TraceablePolicyConfigServiceGrpc;
import ai.traceable.policy.config.service.v1.UpdateRequest;
import ai.traceable.policy.config.service.v1.UpdateResponse;
import ai.traceable.policy.config.service.v1.UpsertRequest;
import ai.traceable.policy.config.service.v1.UpsertResponse;
import ai.traceable.policy.config.service.validation.RequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class TraceablePolicyConfigService
    extends TraceablePolicyConfigServiceGrpc.TraceablePolicyConfigServiceImplBase {

  private final TraceablePolicyConfigStoreManager traceablePolicyConfigStoreManager;
  private final RequestValidator requestValidator;

  @Inject
  TraceablePolicyConfigService(
      TraceablePolicyConfigStoreManager traceablePolicyConfigStoreManager,
      RequestValidator requestValidator) {
    this.traceablePolicyConfigStoreManager = traceablePolicyConfigStoreManager;
    this.requestValidator = requestValidator;
  }

  @Override
  public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, traceablePolicyConfigStoreManager::update);
  }

  @Override
  public void upsert(UpsertRequest request, StreamObserver<UpsertResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, traceablePolicyConfigStoreManager::upsert);
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, traceablePolicyConfigStoreManager::get);
  }

  @Override
  public void getAll(GetAllRequest request, StreamObserver<GetAllResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, traceablePolicyConfigStoreManager::getAll);
  }

  @Override
  public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, traceablePolicyConfigStoreManager::delete);
  }

  private Exception decorateException(RequestContext requestContext, Exception exception) {
    return Status.fromThrowable(exception)
        .withCause(exception)
        .asException(requestContext.buildTrailers());
  }

  // Define a common method to handle requests
  private <Req, Res> void handleConfigOperation(
      Req request,
      StreamObserver<Res> responseObserver,
      BiFunction<RequestContext, Req, Res> configOperation) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    RequestValidator.validateRequestContext(requestContext);
    try {
      responseObserver.onNext(configOperation.apply(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while processing request: {} with context {}. Error: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }
}
