package ai.traceable.fraud.policy.config.service;

import ai.traceable.fraud.policy.config.service.store.FraudPolicyConfigStoreManager;
import ai.traceable.fraud.policy.config.service.v1.CreateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.CreateFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyConfigServiceGrpc;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListResponse;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.validation.FraudPolicyConfigRequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class FraudPolicyConfigServiceImpl
    extends FraudPolicyConfigServiceGrpc.FraudPolicyConfigServiceImplBase {

  private final FraudPolicyConfigStoreManager fraudPolicyConfigStoreManager;
  private final FraudPolicyConfigRequestValidator requestValidator;

  @Inject
  FraudPolicyConfigServiceImpl(
      FraudPolicyConfigStoreManager fraudPolicyConfigStoreManager,
      FraudPolicyConfigRequestValidator requestValidator) {
    this.fraudPolicyConfigStoreManager = fraudPolicyConfigStoreManager;
    this.requestValidator = requestValidator;
  }

  @Override
  public void createFraudPolicy(
      CreateFraudPolicyRequest request,
      StreamObserver<CreateFraudPolicyResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.requestValidator.validateOrThrow(requestContext, request);
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
  public void updateFraudPolicy(
      UpdateFraudPolicyRequest request,
      StreamObserver<UpdateFraudPolicyResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.requestValidator.validateOrThrow(requestContext, request);
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

  private Exception decorateException(RequestContext requestContext, Exception exception) {
    return Status.fromThrowable(exception)
        .withCause(exception)
        .asException(requestContext.buildTrailers());
  }
}
