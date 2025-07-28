package ai.traceable.fraud.policy.config.service;

import ai.traceable.fraud.policy.config.service.store.ApiAccessAnomalyConfigStoreManager;
import ai.traceable.fraud.policy.config.service.store.FraudPolicyConfigStoreManager;
import ai.traceable.fraud.policy.config.service.store.TemplateConfigStoreManager;
import ai.traceable.fraud.policy.config.service.v1.CreateApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.CreateApiAccessAnomalyConfigResponse;
import ai.traceable.fraud.policy.config.service.v1.CreateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.CreateFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.CreateTemplateRequest;
import ai.traceable.fraud.policy.config.service.v1.CreateTemplateResponse;
import ai.traceable.fraud.policy.config.service.v1.DeleteApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteApiAccessAnomalyConfigResponse;
import ai.traceable.fraud.policy.config.service.v1.DeleteFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.DeleteTemplateRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteTemplateResponse;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyConfigServiceGrpc;
import ai.traceable.fraud.policy.config.service.v1.GetApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.GetApiAccessAnomalyConfigResponse;
import ai.traceable.fraud.policy.config.service.v1.GetApiAccessAnomalyConfigsRequest;
import ai.traceable.fraud.policy.config.service.v1.GetApiAccessAnomalyConfigsResponse;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListResponse;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.GetTemplateListRequest;
import ai.traceable.fraud.policy.config.service.v1.GetTemplateListResponse;
import ai.traceable.fraud.policy.config.service.v1.GetTemplateRequest;
import ai.traceable.fraud.policy.config.service.v1.GetTemplateResponse;
import ai.traceable.fraud.policy.config.service.v1.UpdateApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateApiAccessAnomalyConfigResponse;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.UpdateTemplateRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateTemplateResponse;
import ai.traceable.fraud.policy.config.service.v1.UpsertFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.UpsertFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.UpsertTemplateRequest;
import ai.traceable.fraud.policy.config.service.v1.UpsertTemplateResponse;
import ai.traceable.fraud.policy.config.service.validation.ApiAccessAnomalyConfigServiceRequestValidator;
import ai.traceable.fraud.policy.config.service.validation.FraudPolicyConfigRequestValidator;
import ai.traceable.fraud.policy.config.service.validation.TemplateConfigRequestValidator;
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
  private final FraudPolicyConfigRequestValidator fraudPolicyConfigRequestValidator;
  private final TemplateConfigStoreManager templateConfigStoreManager;
  private final TemplateConfigRequestValidator templateConfigRequestValidator;
  private final ApiAccessAnomalyConfigStoreManager apiAccessAnomalyConfigStoreManager;
  private final ApiAccessAnomalyConfigServiceRequestValidator
      apiAccessAnomalyConfigServiceRequestValidator;

  @Inject
  FraudPolicyConfigServiceImpl(
      FraudPolicyConfigStoreManager fraudPolicyConfigStoreManager,
      FraudPolicyConfigRequestValidator fraudPolicyConfigRequestValidator,
      TemplateConfigStoreManager templateConfigStoreManager,
      TemplateConfigRequestValidator templateConfigRequestValidator,
      ApiAccessAnomalyConfigStoreManager apiAccessAnomalyConfigStoreManager,
      ApiAccessAnomalyConfigServiceRequestValidator apiAccessAnomalyConfigServiceRequestValidator) {
    this.fraudPolicyConfigStoreManager = fraudPolicyConfigStoreManager;
    this.fraudPolicyConfigRequestValidator = fraudPolicyConfigRequestValidator;
    this.templateConfigStoreManager = templateConfigStoreManager;
    this.templateConfigRequestValidator = templateConfigRequestValidator;
    this.apiAccessAnomalyConfigStoreManager = apiAccessAnomalyConfigStoreManager;
    this.apiAccessAnomalyConfigServiceRequestValidator =
        apiAccessAnomalyConfigServiceRequestValidator;
  }

  @Override
  public void createFraudPolicy(
      CreateFraudPolicyRequest request,
      StreamObserver<CreateFraudPolicyResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.fraudPolicyConfigRequestValidator.validateRequestContext(requestContext);
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
      this.fraudPolicyConfigRequestValidator.validateRequestContext(requestContext);
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
      this.fraudPolicyConfigRequestValidator.validateRequestContext(requestContext);
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
      this.fraudPolicyConfigRequestValidator.validateRequestContext(requestContext);
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
  public void createTemplate(
      CreateTemplateRequest request, StreamObserver<CreateTemplateResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.templateConfigRequestValidator.validateRequestContext(requestContext);
      responseObserver.onNext(templateConfigStoreManager.createTemplate(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while creating fraud policy template config for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void updateTemplate(
      UpdateTemplateRequest request, StreamObserver<UpdateTemplateResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.templateConfigRequestValidator.validateRequestContext(requestContext);
      responseObserver.onNext(templateConfigStoreManager.updateTemplate(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while updating fraud policy template config for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void upsertTemplate(
      UpsertTemplateRequest request, StreamObserver<UpsertTemplateResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.templateConfigRequestValidator.validateRequestContext(requestContext);
      responseObserver.onNext(templateConfigStoreManager.upsertTemplate(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while creating fraud policy template config for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void deleteTemplate(
      DeleteTemplateRequest request, StreamObserver<DeleteTemplateResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.templateConfigRequestValidator.validateRequestContext(requestContext);
      responseObserver.onNext(
          templateConfigStoreManager.deleteTemplateList(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while deleting fraud policy template configs for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void getTemplateList(
      GetTemplateListRequest request, StreamObserver<GetTemplateListResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      responseObserver.onNext(
          templateConfigStoreManager.fetchTemplateList(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while fetching fraud policy templates for request: {} with context {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void getTemplate(
      GetTemplateRequest request, StreamObserver<GetTemplateResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      responseObserver.onNext(templateConfigStoreManager.fetchTemplate(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while fetching fraud policy template for request: {} with context {}",
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
