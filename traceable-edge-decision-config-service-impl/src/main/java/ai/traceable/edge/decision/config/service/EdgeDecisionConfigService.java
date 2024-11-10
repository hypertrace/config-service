package ai.traceable.edge.decision.config.service;

import ai.traceable.edge.decision.config.service.store.EdgeDecisionConfigStoreManager;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionConfigServiceGrpc;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigResponse;
import ai.traceable.edge.decision.config.service.v1.GetResolvedEdgeDecisionEngineConfigsRequest;
import ai.traceable.edge.decision.config.service.v1.GetResolvedEdgeDecisionEngineConfigsResponse;
import ai.traceable.edge.decision.config.service.v1.GetScopedResolvedEdgeDecisionConfigRequest;
import ai.traceable.edge.decision.config.service.v1.GetScopedResolvedEdgeDecisionConfigResponse;
import ai.traceable.edge.decision.config.service.v1.UpsertEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.UpsertEdgeDecisionEngineConfigResponse;
import ai.traceable.edge.decision.config.service.validation.RequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.function.BiFunction;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class EdgeDecisionConfigService
    extends EdgeDecisionConfigServiceGrpc.EdgeDecisionConfigServiceImplBase {

  private final EdgeDecisionConfigStoreManager edgeDecisionConfigStoreManager;
  private final RequestValidator requestValidator;

  @Inject
  EdgeDecisionConfigService(
      EdgeDecisionConfigStoreManager edgeDecisionConfigStoreManager,
      RequestValidator requestValidator) {
    this.edgeDecisionConfigStoreManager = edgeDecisionConfigStoreManager;
    this.requestValidator = requestValidator;
  }

  @Override
  public void getResolvedEdgeDecisionEngineConfigs(
      GetResolvedEdgeDecisionEngineConfigsRequest request,
      StreamObserver<GetResolvedEdgeDecisionEngineConfigsResponse> responseObserver) {
    super.getResolvedEdgeDecisionEngineConfigs(request, responseObserver);
  }

  @Override
  public void getScopedResolvedEdgeDecisionConfig(
      GetScopedResolvedEdgeDecisionConfigRequest request,
      StreamObserver<GetScopedResolvedEdgeDecisionConfigResponse> responseObserver) {
    super.getScopedResolvedEdgeDecisionConfig(request, responseObserver);
  }

  @Override
  public void upsertEdgeDecisionEngineConfig(
      UpsertEdgeDecisionEngineConfigRequest request,
      StreamObserver<UpsertEdgeDecisionEngineConfigResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeDecisionConfigStoreManager::upsert);
  }

  @Override
  public void getEdgeDecisionEngineConfig(
      GetEdgeDecisionEngineConfigRequest request,
      StreamObserver<GetEdgeDecisionEngineConfigResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeDecisionConfigStoreManager::get);
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
