package ai.traceable.saved.filter.config.service;

import ai.traceable.saved.filter.config.service.store.SavedFilterStoreManager;
import ai.traceable.saved.filter.config.service.v1.CreateSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.CreateSavedFilterResponse;
import ai.traceable.saved.filter.config.service.v1.DeleteSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.DeleteSavedFilterResponse;
import ai.traceable.saved.filter.config.service.v1.Field;
import ai.traceable.saved.filter.config.service.v1.GetSavedFiltersRequest;
import ai.traceable.saved.filter.config.service.v1.GetSavedFiltersResponse;
import ai.traceable.saved.filter.config.service.v1.SavedFilterServiceGrpc;
import ai.traceable.saved.filter.config.service.v1.UpdateSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.UpdateSavedFilterResponse;
import ai.traceable.saved.filter.config.service.validation.SavedFilterRequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class SavedFilterConfigServiceImpl extends SavedFilterServiceGrpc.SavedFilterServiceImplBase {

  private final SavedFilterStoreManager savedFilterStoreManager;
  private final SavedFilterRequestValidator requestValidator;

  @Inject
  SavedFilterConfigServiceImpl(
      SavedFilterStoreManager savedFilterStoreManager,
      SavedFilterRequestValidator requestValidator) {
    this.savedFilterStoreManager = savedFilterStoreManager;
    this.requestValidator = requestValidator;
  }

  @Override
  public void createSavedFilter(
      CreateSavedFilterRequest request,
      StreamObserver<CreateSavedFilterResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.requestValidator.validateOrThrow(requestContext, request);
      final Set<Field> filterVariables =
          requestValidator.validateFilterCriteriaAndCollectFilterVariables(
              requestContext, request.getScope(), request.getFilterCriteria());
      responseObserver.onNext(
          savedFilterStoreManager.createSavedFilter(requestContext, request, filterVariables));
      responseObserver.onCompleted();
    } catch (final Exception exception) {
      final Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while creating saved filter for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void updateSavedFilter(
      UpdateSavedFilterRequest request,
      StreamObserver<UpdateSavedFilterResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.requestValidator.validateOrThrow(requestContext, request);
      final Set<Field> filterVariables =
          requestValidator.validateFilterCriteriaForUpdateAndCollectFilterVariables(
              requestContext, request);
      responseObserver.onNext(
          savedFilterStoreManager.updateSavedFilter(requestContext, request, filterVariables));
      responseObserver.onCompleted();
    } catch (final Exception exception) {
      final Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while updating saved filter for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void deleteSavedFilter(
      DeleteSavedFilterRequest request,
      StreamObserver<DeleteSavedFilterResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(savedFilterStoreManager.deleteSavedFilter(requestContext, request));
      responseObserver.onCompleted();
    } catch (final Exception exception) {
      final Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error deleting saved filter for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void getSavedFilters(
      GetSavedFiltersRequest request, StreamObserver<GetSavedFiltersResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(savedFilterStoreManager.fetchSavedFilters(requestContext, request));
      responseObserver.onCompleted();
    } catch (final Exception exception) {
      final Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while fetching saved filters for request: {} with context {}",
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
