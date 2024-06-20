package ai.traceable.saved.filter.config.service;

import ai.traceable.saved.filter.config.service.store.SavedFilterStoreManager;
import ai.traceable.saved.filter.config.service.v1.CreateSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.CreateSavedFilterResponse;
import ai.traceable.saved.filter.config.service.v1.DeleteSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.DeleteSavedFilterResponse;
import ai.traceable.saved.filter.config.service.v1.GetSavedFiltersRequest;
import ai.traceable.saved.filter.config.service.v1.GetSavedFiltersResponse;
import ai.traceable.saved.filter.config.service.v1.SavedFilter;
import ai.traceable.saved.filter.config.service.v1.SavedFilterServiceGrpc;
import ai.traceable.saved.filter.config.service.v1.UpdateSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.UpdateSavedFilterResponse;
import ai.traceable.saved.filter.config.service.validation.SavedFilterRequestValidator;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class SavedFilterConfigServiceImpl extends SavedFilterServiceGrpc.SavedFilterServiceImplBase {

  private static final String SYSTEM_SAVED_FILTERS =
      "saved.filter.config.service.default.saved.filters";

  private final SavedFilterStoreManager savedFilterStoreManager;
  private final SavedFilterRequestValidator requestValidator;
  private Map<String, SavedFilter> systemSavedFilterIdToFilterMap;

  @Inject
  SavedFilterConfigServiceImpl(
      SavedFilterStoreManager savedFilterStoreManager,
      SavedFilterRequestValidator requestValidator,
      Config config) {
    this.savedFilterStoreManager = savedFilterStoreManager;
    this.requestValidator = requestValidator;

    buildSystemSavedFilterConfigs(config);
  }

  @Override
  public void createSavedFilter(
      CreateSavedFilterRequest request,
      StreamObserver<CreateSavedFilterResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(savedFilterStoreManager.createSavedFilter(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
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
      responseObserver.onNext(
          savedFilterStoreManager.updateSavedFilter(
              requestContext, request, systemSavedFilterIdToFilterMap));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
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
      responseObserver.onNext(
          savedFilterStoreManager.deleteSavedFilter(
              requestContext, request, systemSavedFilterIdToFilterMap));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
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
      responseObserver.onNext(
          savedFilterStoreManager.fetchSavedFilters(
              requestContext, request, systemSavedFilterIdToFilterMap));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while fetching saved filters for request: {} with context {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  private void buildSystemSavedFilterConfigs(Config config) {
    if (!config.hasPath(SYSTEM_SAVED_FILTERS)) {
      this.systemSavedFilterIdToFilterMap = Collections.emptyMap();
      return;
    }

    List<? extends ConfigObject> systemSavedFilterObjects =
        config.getObjectList(SYSTEM_SAVED_FILTERS);
    this.systemSavedFilterIdToFilterMap =
        buildSystemSavedFilterIdToFilterMap(systemSavedFilterObjects);
  }

  private Map<String, SavedFilter> buildSystemSavedFilterIdToFilterMap(
      List<? extends ConfigObject> configObjects) {
    List<SavedFilter> savedFilters = buildSystemSavedFilters(configObjects);
    return savedFilters.stream()
        .collect(Collectors.toUnmodifiableMap(SavedFilter::getId, Function.identity()));
  }

  private List<SavedFilter> buildSystemSavedFilters(List<? extends ConfigObject> configObjects) {
    return configObjects.stream()
        .map(this::buildSavedFilterFromConfig)
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  private SavedFilter buildSavedFilterFromConfig(ConfigObject configObject) {
    String jsonString = configObject.render();
    SavedFilter.Builder builder = SavedFilter.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }

  private Exception decorateException(RequestContext requestContext, Exception exception) {
    return Status.fromThrowable(exception)
        .withCause(exception)
        .asException(requestContext.buildTrailers());
  }
}
