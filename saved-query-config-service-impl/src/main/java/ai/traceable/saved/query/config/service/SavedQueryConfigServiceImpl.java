package ai.traceable.saved.query.config.service;

import ai.traceable.saved.query.config.service.migration.SavedQueryDataMigration;
import ai.traceable.saved.query.config.service.store.SavedQueryStoreManager;
import ai.traceable.saved.query.config.service.v1.CreateSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.CreateSavedQueryResponse;
import ai.traceable.saved.query.config.service.v1.DeleteSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.DeleteSavedQueryResponse;
import ai.traceable.saved.query.config.service.v1.GetAllSavedQueryUsersRequest;
import ai.traceable.saved.query.config.service.v1.GetAllSavedQueryUsersResponse;
import ai.traceable.saved.query.config.service.v1.GetSavedQueriesRequest;
import ai.traceable.saved.query.config.service.v1.GetSavedQueriesResponse;
import ai.traceable.saved.query.config.service.v1.SavedQueryServiceGrpc;
import ai.traceable.saved.query.config.service.v1.UpdateSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.UpdateSavedQueryResponse;
import ai.traceable.saved.query.config.service.validation.SavedQueryRequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class SavedQueryConfigServiceImpl extends SavedQueryServiceGrpc.SavedQueryServiceImplBase {

  private final SavedQueryStoreManager savedQueryStoreManager;
  private final SavedQueryRequestValidator requestValidator;

  @Inject
  SavedQueryConfigServiceImpl(
      SavedQueryStoreManager savedQueryStoreManager,
      SavedQueryRequestValidator requestValidator,
      SavedQueryDataMigration savedQueryDataMigration,
      SavedQueryDataMigrationConfig defaultSavedQueryConfig) {
    this.savedQueryStoreManager = savedQueryStoreManager;
    this.requestValidator = requestValidator;

    if (defaultSavedQueryConfig.isSavedQueryUserDataMigrationEnabled()) {
      savedQueryDataMigration.migrate();
    }
  }

  @Override
  public void createSavedQuery(
      CreateSavedQueryRequest request, StreamObserver<CreateSavedQueryResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(savedQueryStoreManager.createSavedQuery(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while creating saved query for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void updateSavedQuery(
      UpdateSavedQueryRequest request, StreamObserver<UpdateSavedQueryResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(savedQueryStoreManager.updateSavedQuery(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while updating saved query for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void deleteSavedQuery(
      DeleteSavedQueryRequest request, StreamObserver<DeleteSavedQueryResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(savedQueryStoreManager.deleteSavedQuery(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error deleting saved query for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void getSavedQueries(
      GetSavedQueriesRequest request, StreamObserver<GetSavedQueriesResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(savedQueryStoreManager.fetchSavedQueries(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while fetching saved queries for request: {} with context {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void getAllSavedQueryUsers(
      GetAllSavedQueryUsersRequest request,
      StreamObserver<GetAllSavedQueryUsersResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      responseObserver.onNext(savedQueryStoreManager.getAllSavedQueryUsers(requestContext));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.error(
          "Error while fetching list of saved query users with request context {}",
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
