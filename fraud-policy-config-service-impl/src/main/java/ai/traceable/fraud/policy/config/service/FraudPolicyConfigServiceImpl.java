package ai.traceable.fraud.policy.config.service;

import ai.traceable.fraud.policy.config.service.store.ApiAccessAnomalyConfigStoreManager;
import ai.traceable.fraud.policy.config.service.store.FraudPolicyConfigStoreManager;
import ai.traceable.fraud.policy.config.service.v1.CreateApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.CreateApiAccessAnomalyConfigResponse;
import ai.traceable.fraud.policy.config.service.v1.CreateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.CreateFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.DeleteApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteApiAccessAnomalyConfigResponse;
import ai.traceable.fraud.policy.config.service.v1.DeleteFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyConfigServiceGrpc;
import ai.traceable.fraud.policy.config.service.v1.GetApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.GetApiAccessAnomalyConfigResponse;
import ai.traceable.fraud.policy.config.service.v1.GetApiAccessAnomalyConfigsRequest;
import ai.traceable.fraud.policy.config.service.v1.GetApiAccessAnomalyConfigsResponse;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListResponse;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.UpdateApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateApiAccessAnomalyConfigResponse;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.UpsertFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.UpsertFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.validation.FraudPolicyConfigRequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class FraudPolicyConfigServiceImpl
    extends FraudPolicyConfigServiceGrpc.FraudPolicyConfigServiceImplBase {

  private final FraudPolicyConfigStoreManager fraudPolicyConfigStoreManager;

  private final ApiAccessAnomalyConfigStoreManager apiAccessAnomalyConfigStoreManager;

  @Inject
  FraudPolicyConfigServiceImpl(
      FraudPolicyConfigStoreManager fraudPolicyConfigStoreManager,
      ApiAccessAnomalyConfigStoreManager apiAccessAnomalyConfigStoreManager) {
    this.fraudPolicyConfigStoreManager = fraudPolicyConfigStoreManager;
    this.apiAccessAnomalyConfigStoreManager = apiAccessAnomalyConfigStoreManager;
  }

  @Override
  public void createFraudPolicy(
      CreateFraudPolicyRequest request,
      StreamObserver<CreateFraudPolicyResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      FraudPolicyConfigRequestValidator.validateRequestContext(requestContext);
      FraudPolicyConfigRequestValidator.validateRawSQLQuery(request.getFraudPolicy());
      responseObserver.onNext(
          fraudPolicyConfigStoreManager.createFraudPolicy(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while creating fraud policy config for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void upsertFraudPolicy(
      UpsertFraudPolicyRequest request,
      StreamObserver<UpsertFraudPolicyResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      FraudPolicyConfigRequestValidator.validateRequestContext(requestContext);
      FraudPolicyConfigRequestValidator.validateRawSQLQuery(request.getFraudPolicy());
      responseObserver.onNext(
          fraudPolicyConfigStoreManager.upsertFraudPolicy(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while creating fraud policy config for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void updateFraudPolicy(
      UpdateFraudPolicyRequest request,
      StreamObserver<UpdateFraudPolicyResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      FraudPolicyConfigRequestValidator.validateRequestContext(requestContext);
      responseObserver.onNext(
          fraudPolicyConfigStoreManager.updateFraudPolicy(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while updating fraud policy config for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void deleteFraudPolicy(
      DeleteFraudPolicyRequest request,
      StreamObserver<DeleteFraudPolicyResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      FraudPolicyConfigRequestValidator.validateRequestContext(requestContext);
      responseObserver.onNext(
          fraudPolicyConfigStoreManager.deleteFraudPolicyList(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while deleting fraud policy configs for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void getFraudPolicyList(
      GetFraudPolicyListRequest request,
      StreamObserver<GetFraudPolicyListResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      responseObserver.onNext(
          fraudPolicyConfigStoreManager.fetchFraudPolicyList(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while fetching fraud policies for request: {} with context {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void getFraudPolicy(
      GetFraudPolicyRequest request, StreamObserver<GetFraudPolicyResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      responseObserver.onNext(
          fraudPolicyConfigStoreManager.fetchFraudPolicy(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while fetching fraud policy for request: {} with context {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void getApiAccessAnomalyConfigs(
      GetApiAccessAnomalyConfigsRequest request,
      io.grpc.stub.StreamObserver<GetApiAccessAnomalyConfigsResponse> responseObserver) {
    handleConfigOperation(
        request, responseObserver, apiAccessAnomalyConfigStoreManager::getApiAccessAnomalyConfigs);
  }

  @Override
  public void getApiAccessAnomalyConfig(
      GetApiAccessAnomalyConfigRequest request,
      io.grpc.stub.StreamObserver<GetApiAccessAnomalyConfigResponse> responseObserver) {
    handleConfigOperation(
        request, responseObserver, apiAccessAnomalyConfigStoreManager::getApiAccessAnomalyConfig);
  }

  @Override
  public void createApiAccessAnomalyConfig(
      CreateApiAccessAnomalyConfigRequest request,
      io.grpc.stub.StreamObserver<CreateApiAccessAnomalyConfigResponse> responseObserver) {
    handleConfigOperation(
        request,
        responseObserver,
        apiAccessAnomalyConfigStoreManager::createApiAccessAnomalyConfig);
  }

  @Override
  public void updateApiAccessAnomalyConfig(
      UpdateApiAccessAnomalyConfigRequest request,
      io.grpc.stub.StreamObserver<UpdateApiAccessAnomalyConfigResponse> responseObserver) {
    handleConfigOperation(
        request,
        responseObserver,
        apiAccessAnomalyConfigStoreManager::updateApiAccessAnomalyConfig);
  }

  @Override
  public void deleteApiAccessAnomalyConfig(
      DeleteApiAccessAnomalyConfigRequest request,
      io.grpc.stub.StreamObserver<DeleteApiAccessAnomalyConfigResponse> responseObserver) {
    handleConfigOperation(
        request,
        responseObserver,
        apiAccessAnomalyConfigStoreManager::deleteApiAccessAnomalyConfig);
  }

  private Exception decorateException(RequestContext requestContext, Exception exception) {
    return Status.fromThrowable(exception)
        .withCause(exception)
        .asException(requestContext.buildTrailers());
  }

  // Define a common method to handle requests
  private <Req, Res> void handleConfigOperation(
      Req request,
      io.grpc.stub.StreamObserver<Res> responseObserver,
      BiFunction<RequestContext, Req, Res> configOperation) {

    RequestContext requestContext = RequestContext.CURRENT.get();
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
