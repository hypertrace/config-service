package ai.traceable.fraud.datamodel.derivation.config.service;

import ai.traceable.fraud.datamodel.derivation.config.service.store.FraudDataModelDerivationConfigStoreManager;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.CreateDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.CreateDerivationConfigResponse;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.FraudDataModelDerivationConfigServiceGrpc;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigResponse;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigsRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigsResponse;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.UpdateDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.UpdateDerivationConfigResponse;
import ai.traceable.fraud.datamodel.derivation.config.service.validation.FraudDataModelDerivationConfigRequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class FraudDataModelDerivationConfigServiceImpl
    extends FraudDataModelDerivationConfigServiceGrpc
        .FraudDataModelDerivationConfigServiceImplBase {

  private final FraudDataModelDerivationConfigStoreManager
      fraudDataModelDerivationConfigStoreManager;
  private final FraudDataModelDerivationConfigRequestValidator requestValidator;

  @Inject
  FraudDataModelDerivationConfigServiceImpl(
      FraudDataModelDerivationConfigStoreManager fraudDataModelDerivationConfigStoreManager,
      FraudDataModelDerivationConfigRequestValidator requestValidator) {
    this.fraudDataModelDerivationConfigStoreManager = fraudDataModelDerivationConfigStoreManager;
    this.requestValidator = requestValidator;
  }

  @Override
  public void createDerivationConfig(
      CreateDerivationConfigRequest request,
      StreamObserver<CreateDerivationConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          fraudDataModelDerivationConfigStoreManager.createDerivationConfig(
              requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while creating derivation config for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void updateDerivationConfig(
      UpdateDerivationConfigRequest request,
      StreamObserver<UpdateDerivationConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          fraudDataModelDerivationConfigStoreManager.updateDerivationConfig(
              requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while updating derivation config for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void getDerivationConfigs(
      GetDerivationConfigsRequest request,
      StreamObserver<GetDerivationConfigsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      responseObserver.onNext(
          fraudDataModelDerivationConfigStoreManager.fetchDerivedConfigs(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while fetching fraud datamodel derivation configs for request: {} with context {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void getDerivationConfig(
      GetDerivationConfigRequest request,
      StreamObserver<GetDerivationConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      responseObserver.onNext(
          fraudDataModelDerivationConfigStoreManager.fetchDerivedConfig(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while fetching fraud datamodel derivation configs for request: {} with context {}",
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
