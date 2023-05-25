package ai.traceable.saved.filter.config.service.store;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.saved.filter.config.service.v1.CreateSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.CreateSavedFilterResponse;
import ai.traceable.saved.filter.config.service.v1.DeleteSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.DeleteSavedFilterResponse;
import ai.traceable.saved.filter.config.service.v1.GetSavedFiltersRequest;
import ai.traceable.saved.filter.config.service.v1.GetSavedFiltersResponse;
import ai.traceable.saved.filter.config.service.v1.SavedFilter;
import ai.traceable.saved.filter.config.service.v1.UpdateSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.UpdateSavedFilterResponse;
import io.grpc.Status;
import io.grpc.StatusException;
import java.util.List;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class SavedFilterStoreManager {

  private final SavedFilterConfigStore savedFilterConfigStore;
  private final TimestampConverter timestampConverter;
  private final UuidGenerator uuidGenerator;

  @Inject
  public SavedFilterStoreManager(
      SavedFilterConfigStore savedFilterConfigStore,
      TimestampConverter timestampConverter,
      UuidGenerator uuidGenerator) {
    this.savedFilterConfigStore = savedFilterConfigStore;
    this.timestampConverter = timestampConverter;
    this.uuidGenerator = uuidGenerator;
  }

  public CreateSavedFilterResponse createSavedFilter(
      RequestContext requestContext, CreateSavedFilterRequest request) {
    SavedFilter newSavedFilter =
        SavedFilter.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setName(request.getName())
            .setScope(request.getScope())
            .setVisibility(request.getVisibility())
            .setFilterCriteria(request.getFilterCriteria())
            .setCreatedByUserId(requestContext.getUserId().orElseThrow())
            .build();
    ContextualConfigObject<SavedFilter> configObject =
        savedFilterConfigStore.upsertObject(requestContext, newSavedFilter);
    SavedFilter savedFilter = buildSavedFilterFromConfigObject(configObject);
    return CreateSavedFilterResponse.newBuilder().setSavedFilter(savedFilter).build();
  }

  public UpdateSavedFilterResponse updateSavedFilter(
      RequestContext requestContext, UpdateSavedFilterRequest request) throws StatusException {
    SavedFilter existingSavedFilter =
        fetchExistingSavedFilterOrThrow(request.getId(), requestContext);
    validateUserPermission(requestContext, existingSavedFilter);
    SavedFilter updatedSavedFilter =
        SavedFilter.newBuilder(existingSavedFilter)
            .setName(request.getName())
            .setVisibility(request.getVisibility())
            .setFilterCriteria(request.getFilterCriteria())
            .build();
    ContextualConfigObject<SavedFilter> configObject =
        savedFilterConfigStore.upsertObject(requestContext, updatedSavedFilter);
    SavedFilter savedFilter = buildSavedFilterFromConfigObject(configObject);
    return UpdateSavedFilterResponse.newBuilder().setSavedFilter(savedFilter).build();
  }

  public DeleteSavedFilterResponse deleteSavedFilter(
      RequestContext requestContext, DeleteSavedFilterRequest request) throws StatusException {
    SavedFilter existingSavedFilter =
        fetchExistingSavedFilterOrThrow(request.getId(), requestContext);
    validateUserPermission(requestContext, existingSavedFilter);
    this.savedFilterConfigStore
        .deleteObject(requestContext, request.getId())
        .orElseThrow(Status.NOT_FOUND::asRuntimeException);
    return DeleteSavedFilterResponse.newBuilder().build();
  }

  public GetSavedFiltersResponse fetchSavedFilters(
      RequestContext requestContext, GetSavedFiltersRequest request) {
    List<SavedFilter> savedFilters =
        savedFilterConfigStore.getAllConfigData(requestContext, request);
    return GetSavedFiltersResponse.newBuilder().addAllSavedFilters(savedFilters).build();
  }

  private SavedFilter buildSavedFilterFromConfigObject(
      ContextualConfigObject<SavedFilter> configObject) {
    return SavedFilter.newBuilder(configObject.getData())
        .setCreatedTimestamp(timestampConverter.convert(configObject.getCreationTimestamp()))
        .setUpdatedTimestamp(timestampConverter.convert(configObject.getLastUpdatedTimestamp()))
        .build();
  }

  private SavedFilter fetchExistingSavedFilterOrThrow(String id, RequestContext requestContext)
      throws StatusException {
    return savedFilterConfigStore
        .getData(requestContext, id)
        .orElseThrow(Status.NOT_FOUND::asException);
  }

  // ToDo ENG-31204 Move this validation into an auth interceptor
  private void validateUserPermission(
      RequestContext requestContext, SavedFilter existingSavedFilter) throws StatusException {
    if (!requestContext
        .getUserId()
        .orElseThrow()
        .equals(existingSavedFilter.getCreatedByUserId())) {
      throw Status.PERMISSION_DENIED.asException();
    }
  }
}
