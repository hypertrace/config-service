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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
      RequestContext requestContext,
      UpdateSavedFilterRequest request,
      Map<String, SavedFilter> systemSavedFilterIdToFilterMap)
      throws StatusException {
    String filterId = request.getId();
    if (systemSavedFilterIdToFilterMap.containsKey(filterId)) {
      SavedFilter savedFilter =
          updateSystemSavedFilter(
              requestContext, request, systemSavedFilterIdToFilterMap.get(filterId));
      return UpdateSavedFilterResponse.newBuilder().setSavedFilter(savedFilter).build();
    }
    SavedFilter existingSavedFilter = fetchExistingSavedFilterOrThrow(filterId, requestContext);
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
      RequestContext requestContext,
      DeleteSavedFilterRequest request,
      Map<String, SavedFilter> systemSavedFilterIdToFilterMap)
      throws StatusException {
    if (systemSavedFilterIdToFilterMap.containsKey(request.getId())) {
      throw Status.PERMISSION_DENIED.asRuntimeException();
    }
    SavedFilter existingSavedFilter =
        fetchExistingSavedFilterOrThrow(request.getId(), requestContext);
    this.savedFilterConfigStore
        .deleteObject(requestContext, request.getId())
        .orElseThrow(Status.NOT_FOUND::asRuntimeException);
    return DeleteSavedFilterResponse.newBuilder().build();
  }

  public GetSavedFiltersResponse fetchSavedFilters(
      RequestContext requestContext,
      GetSavedFiltersRequest request,
      Map<String, SavedFilter> systemSavedFilterIdToFilterMap) {
    List<SavedFilter> userSavedFilters =
        savedFilterConfigStore.getAllConfigData(requestContext, request);
    List<SavedFilter> savedFilters =
        filterSavedFilters(
            reorderSavedFilters(userSavedFilters, systemSavedFilterIdToFilterMap),
            request.getScope());
    return GetSavedFiltersResponse.newBuilder().addAllSavedFilters(savedFilters).build();
  }

  private SavedFilter updateSystemSavedFilter(
      RequestContext requestContext,
      UpdateSavedFilterRequest request,
      SavedFilter existingSystemSavedFilter) {
    SavedFilter updatedSystemSavedFilter =
        SavedFilter.newBuilder(existingSystemSavedFilter)
            .setFilterCriteria(request.getFilterCriteria())
            .setDisabled(request.getDisabled())
            .build();
    ContextualConfigObject<SavedFilter> configObject =
        savedFilterConfigStore.upsertObject(requestContext, updatedSystemSavedFilter);
    return buildSavedFilterFromConfigObject(configObject);
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

  private List<SavedFilter> filterSavedFilters(List<SavedFilter> savedFilters, String scope) {
    if (scope.isEmpty()) {
      return savedFilters;
    }
    return savedFilters.stream()
        .filter(
            savedFilter -> savedFilter.getScope().equals(scope)) // Get all filters of desired scope
        .collect(Collectors.toUnmodifiableList());
  }

  private List<SavedFilter> reorderSavedFilters(
      List<SavedFilter> userSavedFilters, Map<String, SavedFilter> systemSavedFilterIdToFilterMap) {
    List<SavedFilter> savedFilters = new ArrayList<>();
    List<SavedFilter> overriddenSystemSavedFilters = new ArrayList<>();
    Set<String> overriddenSystemSavedFilterIds = new HashSet<>();

    for (SavedFilter userSavedFilter : userSavedFilters) {
      String id = userSavedFilter.getId();
      if (systemSavedFilterIdToFilterMap.containsKey(id)) {
        overriddenSystemSavedFilters.add(userSavedFilter);
        overriddenSystemSavedFilterIds.add(id);
      } else {
        savedFilters.add(userSavedFilter);
      }
    }

    List<SavedFilter> nonOverriddenSystemSavedFilters =
        systemSavedFilterIdToFilterMap.values().stream()
            .filter(savedFilter -> !overriddenSystemSavedFilterIds.contains(savedFilter.getId()))
            .collect(Collectors.toUnmodifiableList());

    savedFilters.addAll(overriddenSystemSavedFilters);
    savedFilters.addAll(nonOverriddenSystemSavedFilters);
    return savedFilters;
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
