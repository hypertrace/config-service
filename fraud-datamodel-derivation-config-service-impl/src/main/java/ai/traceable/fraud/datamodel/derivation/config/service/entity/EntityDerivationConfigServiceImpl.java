package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.DeleteEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.DeleteEntityDerivationConfigResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigServiceGrpc;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigSummariesRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigSummariesResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.UpdateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.UpdateEntityDerivationConfigResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class EntityDerivationConfigServiceImpl
    extends EntityDerivationConfigServiceGrpc.EntityDerivationConfigServiceImplBase {

  private final EntityDerivationConfigStoreManager storeManager;
  private final EntityDerivationConfigRequestValidator validator;

  @Inject
  public EntityDerivationConfigServiceImpl(
      EntityDerivationConfigStoreManager storeManager,
      EntityDerivationConfigRequestValidator validator) {
    this.storeManager = storeManager;
    this.validator = validator;
  }

  @Override
  public void createEntityDerivationConfig(
      CreateEntityDerivationConfigRequest request,
      StreamObserver<CreateEntityDerivationConfigResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      validator.validateCreateRequest(request, ctx);
      responseObserver.onNext(storeManager.createEntityDerivationConfig(ctx, request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      Exception decoratedException = decorateException(ctx, e);
      log.warn(
          "Error while creating entity derivation config for request: {} with context: {}",
          request,
          ctx,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void updateEntityDerivationConfig(
      UpdateEntityDerivationConfigRequest request,
      StreamObserver<UpdateEntityDerivationConfigResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      validator.validateUpdateRequest(request, ctx);
      responseObserver.onNext(storeManager.updateEntityDerivationConfig(ctx, request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      Exception decoratedException = decorateException(ctx, e);
      log.warn(
          "Error while updating entity derivation config for request: {} with context: {}",
          request,
          ctx,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void getEntityDerivationConfigSummaries(
      GetEntityDerivationConfigSummariesRequest request,
      StreamObserver<GetEntityDerivationConfigSummariesResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      validator.validateRequestContext(ctx);
      responseObserver.onNext(storeManager.getEntityDerivationConfigSummaries(ctx, request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      Exception decoratedException = decorateException(ctx, e);
      log.warn(
          "Error while fetching entity derivation config summaries for request: {} with context: {}",
          request,
          ctx,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void getEntityDerivationConfigs(
      GetEntityDerivationConfigsRequest request,
      StreamObserver<GetEntityDerivationConfigsResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      validator.validateRequestContext(ctx);
      responseObserver.onNext(storeManager.getEntityDerivationConfigs(ctx, request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      Exception decoratedException = decorateException(ctx, e);
      log.warn(
          "Error while fetching entity derivation configs for request: {} with context: {}",
          request,
          ctx,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void deleteEntityDerivationConfig(
      DeleteEntityDerivationConfigRequest request,
      StreamObserver<DeleteEntityDerivationConfigResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      validator.validateDeleteRequest(request, ctx);
      responseObserver.onNext(storeManager.deleteEntityDerivationConfig(ctx, request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      Exception decoratedException = decorateException(ctx, e);
      log.warn(
          "Error while deleting entity derivation config for request: {} with context: {}",
          request,
          ctx,
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
