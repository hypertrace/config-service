package ai.traceable.saved.filter.config.service.store;

import ai.traceable.saved.filter.config.service.v1.GetSavedFiltersRequest;
import ai.traceable.saved.filter.config.service.v1.SavedFilter;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class SavedFilterConfigStore
    extends IdentifiedObjectStoreWithFilter<SavedFilter, GetSavedFiltersRequest> {

  private static final String SAVED_FILTER_RESOURCE_NAME = "saved-filter";
  private static final String SAVED_FILTER_CONFIG_RESOURCE_NAMESPACE = "saved-filter-config";

  @Inject
  public SavedFilterConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        SAVED_FILTER_CONFIG_RESOURCE_NAMESPACE,
        SAVED_FILTER_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<SavedFilter> buildDataFromValue(Value value) {
    SavedFilter.Builder savedFilterBuilder = SavedFilter.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, savedFilterBuilder);
    return Optional.of(savedFilterBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(SavedFilter savedFilter) {
    return ConfigProtoConverter.convertToValue(savedFilter);
  }

  @Override
  protected String getContextFromData(SavedFilter savedFilter) {
    return savedFilter.getId();
  }

  @Override
  protected Optional<SavedFilter> filterConfigData(
      SavedFilter data, GetSavedFiltersRequest request) {
    return request.getScope().equals(data.getScope()) ? Optional.of(data) : Optional.empty();
  }

  @Override
  public List<SavedFilter> getAllConfigData(
      RequestContext requestContext, GetSavedFiltersRequest request) {
    List<SavedFilter> savedFilters = super.getAllConfigData(requestContext, request);
    return savedFilters.stream()
        .filter(
            savedFilter ->
                savedFilter.getVisibility().hasPublic()
                    || (savedFilter.getVisibility().hasPrivate()
                        && savedFilter
                            .getCreatedByUserId()
                            .equals(requestContext.getUserId().orElseThrow())))
        .collect((Collectors.toUnmodifiableList()));
  }
}
