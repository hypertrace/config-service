package ai.traceable.edge.decision.config.service;

import ai.traceable.edge.decision.config.service.store.EdgeAttributionRuleStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeCustomResponseStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionConfigStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionRuleStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionSpecStoreManager;
import ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigResolver;
import ai.traceable.edge.decision.config.service.supplier.StoredEdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeAttributionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeAttributionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeCustomResponseRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeCustomResponseResponse;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionEngineConfigResponse;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionSpecRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionSpecResponse;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeAttributionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeAttributionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeCustomResponseRequest;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeCustomResponseResponse;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeDecisionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeDecisionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeDecisionSpecRequest;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeDecisionSpecResponse;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionConfigServiceGrpc;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeAttributionRulesRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeAttributionRulesResponse;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeCustomResponsesRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeCustomResponsesResponse;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionEngineConfigResponse;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionRulesRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionRulesResponse;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionSpecsRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionSpecsResponse;
import ai.traceable.edge.decision.config.service.v1.GetEdgeAttributionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeAttributionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.GetEdgeCustomResponseRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeCustomResponseResponse;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionConfigsFilter;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigResponse;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionSpecRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionSpecResponse;
import ai.traceable.edge.decision.config.service.v1.GetResolvedEdgeDecisionEngineConfigsRequest;
import ai.traceable.edge.decision.config.service.v1.GetResolvedEdgeDecisionEngineConfigsResponse;
import ai.traceable.edge.decision.config.service.v1.GetScopedResolvedEdgeDecisionConfigRequest;
import ai.traceable.edge.decision.config.service.v1.GetScopedResolvedEdgeDecisionConfigResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeAttributionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeAttributionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeCustomResponseRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeCustomResponseResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionEngineConfigResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionSpecRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionSpecResponse;
import ai.traceable.edge.decision.config.service.validation.RequestValidator;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class EdgeDecisionConfigService
    extends EdgeDecisionConfigServiceGrpc.EdgeDecisionConfigServiceImplBase {
  private final EdgeDecisionRuleStoreManager edgeDecisionRuleStoreManager;
  private final EdgeDecisionSpecStoreManager edgeDecisionSpecStoreManager;
  private final EdgeDecisionConfigStoreManager edgeDecisionConfigStoreManager;
  private final EdgeAttributionRuleStoreManager edgeAttributionRuleStoreManager;
  private final EdgeCustomResponseStoreManager edgeCustomResponseStoreManager;
  private final StoredEdgeDecisionEngineConfigSupplier storedEdgeDecisionEngineConfigSupplier;
  private final EdgeDecisionEngineConfigResolver configResolver;
  private final RequestValidator requestValidator;

  @Inject
  EdgeDecisionConfigService(
      EdgeDecisionRuleStoreManager edgeDecisionRuleStoreManager,
      EdgeDecisionSpecStoreManager edgeDecisionSpecStoreManager,
      EdgeDecisionConfigStoreManager edgeDecisionConfigStoreManager,
      EdgeAttributionRuleStoreManager edgeAttributionRuleStoreManager,
      EdgeCustomResponseStoreManager edgeCustomResponseStoreManager,
      StoredEdgeDecisionEngineConfigSupplier storedEdgeDecisionEngineConfigSupplier,
      EdgeDecisionEngineConfigResolver configResolver,
      RequestValidator requestValidator) {
    this.edgeDecisionRuleStoreManager = edgeDecisionRuleStoreManager;
    this.edgeDecisionSpecStoreManager = edgeDecisionSpecStoreManager;
    this.edgeDecisionConfigStoreManager = edgeDecisionConfigStoreManager;
    this.edgeAttributionRuleStoreManager = edgeAttributionRuleStoreManager;
    this.edgeCustomResponseStoreManager = edgeCustomResponseStoreManager;
    this.storedEdgeDecisionEngineConfigSupplier = storedEdgeDecisionEngineConfigSupplier;
    this.configResolver = configResolver;
    this.requestValidator = requestValidator;
  }

  @Override
  public void getResolvedEdgeDecisionEngineConfigs(
      GetResolvedEdgeDecisionEngineConfigsRequest request,
      StreamObserver<GetResolvedEdgeDecisionEngineConfigsResponse> responseObserver) {
    handleConfigOperation(
        request,
        responseObserver,
        (requestContext, getResolvedEdgeDecisionEngineConfigsRequest) -> {
          EdgeDecisionEngineConfig finalConfig =
              configResolver.getResolvedEdgeDecisionEngineConfig(
                  requestContext, getResolvedEdgeDecisionEngineConfigsRequest);
          return GetResolvedEdgeDecisionEngineConfigsResponse.newBuilder()
              .setEdgeDecisionEngineConfig(finalConfig)
              .build();
        });
  }

  @Override
  public void getScopedResolvedEdgeDecisionConfig(
      GetScopedResolvedEdgeDecisionConfigRequest request,
      StreamObserver<GetScopedResolvedEdgeDecisionConfigResponse> responseObserver) {
    super.getScopedResolvedEdgeDecisionConfig(request, responseObserver);
  }

  @Override
  public void createEdgeDecisionEngineConfig(
      CreateEdgeDecisionEngineConfigRequest request,
      StreamObserver<CreateEdgeDecisionEngineConfigResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeDecisionConfigStoreManager::create);
  }

  @Override
  public void updateEdgeDecisionEngineConfig(
      UpdateEdgeDecisionEngineConfigRequest request,
      StreamObserver<UpdateEdgeDecisionEngineConfigResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeDecisionConfigStoreManager::update);
  }

  @Override
  public void getEdgeDecisionEngineConfig(
      GetEdgeDecisionEngineConfigRequest request,
      StreamObserver<GetEdgeDecisionEngineConfigResponse> responseObserver) {
    handleConfigOperation(
        request,
        responseObserver,
        (requestContext, request1) -> {
          EdgeDecisionEngineConfig config =
              storedEdgeDecisionEngineConfigSupplier.get(
                  requestContext, GetEdgeDecisionConfigsFilter.getDefaultInstance());
          return GetEdgeDecisionEngineConfigResponse.newBuilder()
              .setEdgeDecisionEngineConfig(config)
              .build();
        });
  }

  @Override
  public void getAllEdgeDecisionEngineConfig(
      GetAllEdgeDecisionEngineConfigRequest request,
      StreamObserver<GetAllEdgeDecisionEngineConfigResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeDecisionConfigStoreManager::getAll);
  }

  @Override
  public void createEdgeDecisionRule(
      CreateEdgeDecisionRuleRequest request,
      StreamObserver<CreateEdgeDecisionRuleResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeDecisionRuleStoreManager::create);
  }

  @Override
  public void updateEdgeDecisionRule(
      UpdateEdgeDecisionRuleRequest request,
      StreamObserver<UpdateEdgeDecisionRuleResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeDecisionRuleStoreManager::update);
  }

  @Override
  public void getEdgeDecisionRule(
      GetEdgeDecisionRuleRequest request,
      StreamObserver<GetEdgeDecisionRuleResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeDecisionRuleStoreManager::get);
  }

  @Override
  public void getEdgeDecisionSpec(
      GetEdgeDecisionSpecRequest request,
      StreamObserver<GetEdgeDecisionSpecResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeDecisionSpecStoreManager::get);
  }

  @Override
  public void getAllEdgeDecisionRules(
      GetAllEdgeDecisionRulesRequest request,
      StreamObserver<GetAllEdgeDecisionRulesResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeDecisionRuleStoreManager::getAll);
  }

  @Override
  public void deleteEdgeDecisionRule(
      DeleteEdgeDecisionRuleRequest request,
      StreamObserver<DeleteEdgeDecisionRuleResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeDecisionRuleStoreManager::delete);
  }

  @Override
  public void createEdgeDecisionSpec(
      CreateEdgeDecisionSpecRequest request,
      StreamObserver<CreateEdgeDecisionSpecResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeDecisionSpecStoreManager::create);
  }

  @Override
  public void updateEdgeDecisionSpec(
      UpdateEdgeDecisionSpecRequest request,
      StreamObserver<UpdateEdgeDecisionSpecResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeDecisionSpecStoreManager::update);
  }

  @Override
  public void getAllEdgeDecisionSpecs(
      GetAllEdgeDecisionSpecsRequest request,
      StreamObserver<GetAllEdgeDecisionSpecsResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeDecisionSpecStoreManager::getAll);
  }

  @Override
  public void deleteEdgeDecisionSpec(
      DeleteEdgeDecisionSpecRequest request,
      StreamObserver<DeleteEdgeDecisionSpecResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeDecisionSpecStoreManager::delete);
  }

  @Override
  public void createEdgeAttributionRule(
      CreateEdgeAttributionRuleRequest request,
      StreamObserver<CreateEdgeAttributionRuleResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeAttributionRuleStoreManager::create);
  }

  @Override
  public void updateEdgeAttributionRule(
      UpdateEdgeAttributionRuleRequest request,
      StreamObserver<UpdateEdgeAttributionRuleResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeAttributionRuleStoreManager::update);
  }

  @Override
  public void getEdgeAttributionRule(
      GetEdgeAttributionRuleRequest request,
      StreamObserver<GetEdgeAttributionRuleResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeAttributionRuleStoreManager::get);
  }

  @Override
  public void getAllEdgeAttributionRules(
      GetAllEdgeAttributionRulesRequest request,
      StreamObserver<GetAllEdgeAttributionRulesResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeAttributionRuleStoreManager::getAll);
  }

  @Override
  public void deleteEdgeAttributionRule(
      DeleteEdgeAttributionRuleRequest request,
      StreamObserver<DeleteEdgeAttributionRuleResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeAttributionRuleStoreManager::delete);
  }

  @Override
  public void createEdgeCustomResponse(
      CreateEdgeCustomResponseRequest request,
      StreamObserver<CreateEdgeCustomResponseResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeCustomResponseStoreManager::create);
  }

  @Override
  public void updateEdgeCustomResponse(
      UpdateEdgeCustomResponseRequest request,
      StreamObserver<UpdateEdgeCustomResponseResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeCustomResponseStoreManager::update);
  }

  @Override
  public void getEdgeCustomResponse(
      GetEdgeCustomResponseRequest request,
      StreamObserver<GetEdgeCustomResponseResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeCustomResponseStoreManager::get);
  }

  @Override
  public void getAllEdgeCustomResponses(
      GetAllEdgeCustomResponsesRequest request,
      StreamObserver<GetAllEdgeCustomResponsesResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeCustomResponseStoreManager::getAll);
  }

  @Override
  public void deleteEdgeCustomResponse(
      DeleteEdgeCustomResponseRequest request,
      StreamObserver<DeleteEdgeCustomResponseResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, edgeCustomResponseStoreManager::delete);
  }

  private Exception decorateException(RequestContext requestContext, Exception exception) {
    return Status.fromThrowable(exception)
        .withCause(exception)
        .asException(requestContext.buildTrailers());
  }

  // Define a common method to handle requests
  private <Req extends Message, Res extends Message> void handleConfigOperation(
      Req request,
      StreamObserver<Res> responseObserver,
      BiFunction<RequestContext, Req, Res> configOperation) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    RequestValidator.validateRequest(requestContext, request);
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
