package ai.traceable.data.classification.config.service;

import ai.traceable.data.classification.config.service.v1.DataClassificationOverride;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideFilter;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideRule.DataClassificationOverrideScope;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class DataClassificationOverrideStore
    extends IdentifiedObjectStoreWithFilter<
        DataClassificationOverride, DataClassificationOverrideFilter> {
  private static final String
      DATA_CLASSIFICATION_DATA_CLASSIFICATION_OVERRIDE_CONFIG_RESOURCE_NAME =
          "data-classification-override-config";
  private static final String
      DATA_CLASSIFICATION_DATA_CLASSIFICATION_OVERRIDE_CONFIG_RESOURCE_NAMESPACE =
          "data-classification";

  @Inject
  DataClassificationOverrideStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        DATA_CLASSIFICATION_DATA_CLASSIFICATION_OVERRIDE_CONFIG_RESOURCE_NAMESPACE,
        DATA_CLASSIFICATION_DATA_CLASSIFICATION_OVERRIDE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DataClassificationOverride> buildDataFromValue(Value value) {
    try {
      DataClassificationOverride.Builder builder = DataClassificationOverride.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(DataClassificationOverride object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromData(DataClassificationOverride object) {
    return object.getId();
  }

  @Override
  protected Optional<DataClassificationOverride> filterConfigData(
      DataClassificationOverride data, DataClassificationOverrideFilter filter) {
    if (matchesFilter(data, filter)) {
      return Optional.of(data);
    }
    return Optional.empty();
  }

  private boolean matchesFilter(
      DataClassificationOverride override, DataClassificationOverrideFilter filter) {
    switch (filter.getFilterCase()) {
      case ID_FILTER:
        return filter.getIdFilter().getIdsList().isEmpty()
            || filter.getIdFilter().getIdsList().contains(override.getId());
      case SCOPE_FILTER:
        {
          Set<String> allowedEnvironmentIds =
              filter.getScopeFilter().getScopesList().stream()
                  .filter(DataClassificationOverrideScope::hasEnvironmentScope)
                  .map(scope -> scope.getEnvironmentScope().getEnvironmentId())
                  .collect(Collectors.toUnmodifiableSet());

          // An unscoped rule would be a partial match to any filter
          boolean isAllowablePartialMatch =
              !override.getDataClassificationOverrideRule().hasScope()
                  && filter.getScopeFilter().getIncludePartialMatches();
          boolean isEmptyFilter = allowedEnvironmentIds.isEmpty();
          boolean isExactMatch =
              allowedEnvironmentIds.contains(
                  override
                      .getDataClassificationOverrideRule()
                      .getScope()
                      .getEnvironmentScope()
                      .getEnvironmentId());
          return isAllowablePartialMatch || isEmptyFilter || isExactMatch;
        }
      case LOGICAL_AND_FILTER:
        return filter.getLogicalAndFilter().getFiltersList().stream()
            .allMatch(innerFilter -> matchesFilter(override, innerFilter));
      default:
        return true;
    }
  }
}
