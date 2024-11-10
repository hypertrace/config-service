package ai.traceable.edge.bot.config.service;

import ai.traceable.edge.bot.config.service.store.CaptchaSiteKeyConfigStoreManager;
import ai.traceable.edge.bot.config.service.v1.CaptchaSiteKeyConfigServiceGrpc;
import ai.traceable.edge.bot.config.service.v1.DeleteRequest;
import ai.traceable.edge.bot.config.service.v1.DeleteResponse;
import ai.traceable.edge.bot.config.service.v1.GetAllRequest;
import ai.traceable.edge.bot.config.service.v1.GetAllResponse;
import ai.traceable.edge.bot.config.service.v1.GetRequest;
import ai.traceable.edge.bot.config.service.v1.GetResponse;
import ai.traceable.edge.bot.config.service.v1.UpsertRequest;
import ai.traceable.edge.bot.config.service.v1.UpsertResponse;
import ai.traceable.edge.bot.config.service.validation.RequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.function.BiFunction;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class CaptchaSiteKeyConfigService
    extends CaptchaSiteKeyConfigServiceGrpc.CaptchaSiteKeyConfigServiceImplBase {

  private final CaptchaSiteKeyConfigStoreManager captchaSiteKeyConfigStoreManager;
  private final RequestValidator requestValidator;

  @Inject
  CaptchaSiteKeyConfigService(
      CaptchaSiteKeyConfigStoreManager captchaSiteKeyConfigStoreManager,
      RequestValidator requestValidator) {
    this.captchaSiteKeyConfigStoreManager = captchaSiteKeyConfigStoreManager;
    this.requestValidator = requestValidator;
  }

  @Override
  public void upsert(UpsertRequest request, StreamObserver<UpsertResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, captchaSiteKeyConfigStoreManager::upsert);
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, captchaSiteKeyConfigStoreManager::get);
  }

  @Override
  public void getAll(GetAllRequest request, StreamObserver<GetAllResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, captchaSiteKeyConfigStoreManager::getAll);
  }

  @Override
  public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, captchaSiteKeyConfigStoreManager::delete);
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
