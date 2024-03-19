package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeProvenance.FROM_LEGACY_REDACTION_RULE;
import static ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeProvenance.ORPHAN_DATA_TYPE;

import ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeResolutionContext;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest.DataTypeFilter;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
import ai.traceable.data.classification.config.service.v1.SystemDataSetVersion;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@RequiredArgsConstructor(onConstructor_ = @Inject)
class DataTypeManager {
  private final DataTypeStore dataTypeStore;
  private final RedactionRulesDao redactionRulesDao;
  private final DataClassificationConfig dataClassificationConfig;
  private final DataClassificationResolutionCache dataClassificationResolutionCache;
  private final DataTypeResolutionContextComparator dataTypeResolutionContextComparator;

  GetDataTypesResponse getDataTypesMatchingRequest(
      RequestContext requestContext, GetDataTypesRequest request) {
    // We filter and sort on the resolution result regardless of whether we return the resolved
    // value
    List<DataTypeResolutionContext> dataTypeResolutionContexts =
        this.dataClassificationResolutionCache
            .getDataTypeResolutions(
                requestContext,
                this.gatherDataTypes(requestContext, request.getSystemDataSetVersion()))
            .stream()
            .filter(
                resolutionContext ->
                    this.filterDataTypeContext(resolutionContext, request.getFilter()))
            .sorted(this.getComparatorForRequest(request))
            .collect(Collectors.toUnmodifiableList());

    List<DataType> dataTypes =
        dataTypeResolutionContexts.stream()
            .map(
                resolutionContext ->
                    request.getResolveInheritedDetails()
                        ? resolutionContext.getResolvedDataType()
                        : resolutionContext.getOriginalDataType())
            .collect(Collectors.toUnmodifiableList());

    Map<String, DataSet> dataSetsById =
        dataTypeResolutionContexts.stream()
            .map(DataTypeResolutionContext::getDataSets)
            .flatMap(Collection::stream)
            .distinct()
            .collect(ImmutableMap.toImmutableMap(DataSet::getId, Function.identity()));

    return GetDataTypesResponse.newBuilder()
        .addAllDataTypes(dataTypes)
        .putAllReferencedDataSetsById(dataSetsById)
        .build();
  }

  // TODO move other data type operations into manager

  private boolean filterDataTypeContext(
      DataClassificationResolutionCache.DataTypeResolutionContext resolutionContext,
      DataTypeFilter filter) {
    boolean filterResult = true;
    if (filter.hasEnabled()) {
      filterResult &=
          filter.getEnabled() == resolutionContext.getResolvedDataType().getRule().getEnabled();
    }
    if (filter.hasLegacyTypes()) {
      filterResult &=
          filter.getLegacyTypes()
              == resolutionContext.getProvenance().equals(FROM_LEGACY_REDACTION_RULE);
    }
    if (filter.hasOrphanTypes()) {
      filterResult &=
          filter.getOrphanTypes() == resolutionContext.getProvenance().equals(ORPHAN_DATA_TYPE);
    }
    return filterResult;
  }

  private Comparator<DataClassificationResolutionCache.DataTypeResolutionContext>
      getComparatorForRequest(GetDataTypesRequest request) {
    switch (request.getOrdering()) {
      case DATA_TYPE_ORDERING_EVALUATION_PRIORITY:
        return this.dataTypeResolutionContextComparator;
      case DATA_TYPE_ORDERING_UNSPECIFIED:
      default:
        return (o1, o2) -> 0; // Maintain received order
    }
  }

  private List<DataType> gatherDataTypes(
      RequestContext requestContext, SystemDataSetVersion systemDataSetVersion) {
    List<DataType> tenantDataTypes =
        this.dataTypeStore.getAllObjects(requestContext).stream()
            .map(ConfigObject::getData)
            .collect(Collectors.toUnmodifiableList());
    Set<String> tenantDataTypeIds =
        tenantDataTypes.stream().map(DataType::getId).collect(Collectors.toUnmodifiableSet());
    List<DataType> filteredSystemDataTypes =
        this.dataClassificationConfig.getSystemDataTypes(systemDataSetVersion).stream()
            .filter(dataType -> !tenantDataTypeIds.contains(dataType.getId()))
            .collect(Collectors.toUnmodifiableList());
    List<DataType> convertedRedactionRules =
        redactionRulesDao.getAllDataTypesFromRedactionRules(requestContext);

    return ImmutableList.<DataType>builder()
        .addAll(tenantDataTypes)
        .addAll(filteredSystemDataTypes)
        .addAll(convertedRedactionRules)
        .build();
  }
}
