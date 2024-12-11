package ai.traceable.edge.bot.config.service;

import ai.traceable.edge.bot.config.service.store.CaptchaSiteKeyConfigStoreManager;
import ai.traceable.edge.bot.config.service.store.FlowConfigStoreManager;
import ai.traceable.edge.bot.config.service.v1.BotConfigServiceGrpc;
import ai.traceable.edge.bot.config.service.v1.DeleteCaptchaSiteKeyConfigRequest;
import ai.traceable.edge.bot.config.service.v1.DeleteCaptchaSiteKeyConfigResponse;
import ai.traceable.edge.bot.config.service.v1.DeleteFlowConfigRequest;
import ai.traceable.edge.bot.config.service.v1.DeleteFlowConfigResponse;
import ai.traceable.edge.bot.config.service.v1.GetAllCaptchaSiteKeyConfigsRequest;
import ai.traceable.edge.bot.config.service.v1.GetAllCaptchaSiteKeyConfigsResponse;
import ai.traceable.edge.bot.config.service.v1.GetAllFlowConfigsRequest;
import ai.traceable.edge.bot.config.service.v1.GetAllFlowConfigsResponse;
import ai.traceable.edge.bot.config.service.v1.GetCaptchaSiteKeyConfigRequest;
import ai.traceable.edge.bot.config.service.v1.GetCaptchaSiteKeyConfigResponse;
import ai.traceable.edge.bot.config.service.v1.GetFlowConfigRequest;
import ai.traceable.edge.bot.config.service.v1.GetFlowConfigResponse;
import ai.traceable.edge.bot.config.service.v1.UpsertCaptchaSiteKeyConfigRequest;
import ai.traceable.edge.bot.config.service.v1.UpsertCaptchaSiteKeyConfigResponse;
import ai.traceable.edge.bot.config.service.v1.UpsertFlowConfigRequest;
import ai.traceable.edge.bot.config.service.v1.UpsertFlowConfigResponse;
import ai.traceable.edge.bot.config.service.validation.RequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.function.BiFunction;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class BotConfigService extends BotConfigServiceGrpc.BotConfigServiceImplBase {

  private final CaptchaSiteKeyConfigStoreManager captchaSiteKeyConfigStoreManager;
  private final FlowConfigStoreManager flowConfigStoreManager;
  private final RequestValidator requestValidator;

  @Inject
  BotConfigService(
      CaptchaSiteKeyConfigStoreManager captchaSiteKeyConfigStoreManager,
      FlowConfigStoreManager flowConfigStoreManager,
      RequestValidator requestValidator) {
    this.captchaSiteKeyConfigStoreManager = captchaSiteKeyConfigStoreManager;
    this.flowConfigStoreManager = flowConfigStoreManager;
    this.requestValidator = requestValidator;
  }

  @Override
  public void upsertCaptchaSiteKeyConfig(
      UpsertCaptchaSiteKeyConfigRequest request,
      StreamObserver<UpsertCaptchaSiteKeyConfigResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, captchaSiteKeyConfigStoreManager::upsert);
  }

  @Override
  public void getCaptchaSiteKeyConfig(
      GetCaptchaSiteKeyConfigRequest request,
      StreamObserver<GetCaptchaSiteKeyConfigResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, captchaSiteKeyConfigStoreManager::get);
  }

  @Override
  public void getAllCaptchaSiteKeyConfigs(
      GetAllCaptchaSiteKeyConfigsRequest request,
      StreamObserver<GetAllCaptchaSiteKeyConfigsResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, captchaSiteKeyConfigStoreManager::getAll);
  }

  @Override
  public void deleteCaptchaSiteKeyConfig(
      DeleteCaptchaSiteKeyConfigRequest request,
      StreamObserver<DeleteCaptchaSiteKeyConfigResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, captchaSiteKeyConfigStoreManager::delete);
  }

  @Override
  public void upsertFlowConfig(
      UpsertFlowConfigRequest request, StreamObserver<UpsertFlowConfigResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, flowConfigStoreManager::upsert);
  }

  @Override
  public void getFlowConfig(
      GetFlowConfigRequest request, StreamObserver<GetFlowConfigResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, flowConfigStoreManager::get);
  }

  @Override
  public void getAllFlowConfigs(
      GetAllFlowConfigsRequest request,
      StreamObserver<GetAllFlowConfigsResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, flowConfigStoreManager::getAll);
  }

  @Override
  public void deleteFlowConfig(
      DeleteFlowConfigRequest request, StreamObserver<DeleteFlowConfigResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, flowConfigStoreManager::delete);
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
