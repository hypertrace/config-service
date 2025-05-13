package ai.traceable.api.spec.config.service.store;

import ai.traceable.api.spec.config.service.converter.ApiSpecStatusConverter;
import ai.traceable.api.spec.config.service.converter.ApiSpecsResult;
import ai.traceable.api.spec.config.service.v1.ApiSpec;
import ai.traceable.api.spec.config.service.v1.ApiSpecFilter;
import ai.traceable.api.spec.config.service.v1.Pagination;
import ai.traceable.api.spec.config.service.v1.ReferenceApiSpec;
import ai.traceable.api.spec.config.service.v1.Selection;
import ai.traceable.api.spec.config.service.v1.SpecType;
import ai.traceable.api.spec.config.service.v1.StringList;
import ai.traceable.config.utils.TimestampConverter;
import com.google.inject.Inject;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.config.objectstore.ConfigsResponse;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedFilterPushedDownObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.Filter;
import org.hypertrace.config.service.v1.LogicalFilter;
import org.hypertrace.config.service.v1.LogicalOperator;
import org.hypertrace.config.service.v1.RelationalFilter;
import org.hypertrace.config.service.v1.RelationalOperator;
import org.hypertrace.config.service.v1.SortBy;
import org.hypertrace.config.service.v1.SortOrder;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ApiSpecConfigStore
    extends IdentifiedFilterPushedDownObjectStore<
        ApiSpec, ApiSpecFilter, ai.traceable.api.spec.config.service.v1.SortBy> {

  public static final String CREATION_TIMESTAMP = "creationTimestamp";
  private static final String API_SPEC_CONFIG_RESOURCE_NAME = "api-spec";
  private static final String API_SPEC_CONFIG_RESOURCE_NAMESPACE = "api-spec-config";
  public static final String FILE_CONTENT_SHA_256 = "fileContentSha256";
  public static final String SPEC_ID = "specId";
  public static final String SPEC_PATH = "specPath";
  public static final String SPEC_TYPE = "specType";
  public static final String API_INSPECTOR_DISABLED = "apiInspectorDisabled";
  public static final String REFERENCE_TYPE = "referenceType";
  public static final String STATUS = "status";
  public static final String OPENAPI_RESOLUTION_STATE_PATH =
      "apiSpecMetadata.openApiSpecMetadata.openApiSpecResolutionState";
  public static final String NAME = "name";
  public static final String REFERENCE_SPEC_COMPLETE_SPEC_ID_PATH =
      "apiSpecMetadata.openApiSpecMetadata.openApiSpecReferences.resolutionResult.completeOpenApiSpecReference.specId";
  public static final String REFERENCE_SPEC_IN_COMPLETE_SPEC_ID_PATH =
      "apiSpecMetadata.openApiSpecMetadata.openApiSpecReferences.resolutionResult.inCompleteOpenApiSpecReference.specId";
  public static final String REFERENCE_SPEC_RESOLVED_SPEC_PATH =
      "apiSpecMetadata.openApiSpecMetadata.openApiSpecReferences.resolvedSpecPath";
  private final TimestampConverter timestampConverter;
  private final ApiSpecStatusConverter apiSpecStatusConverter;

  @Inject
  public ApiSpecConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      TimestampConverter timestampConverter,
      ConfigChangeEventGenerator configChangeEventGenerator,
      ApiSpecStatusConverter apiSpecStatusConverter,
      ClientConfig clientConfig) {
    super(
        configServiceBlockingStub,
        API_SPEC_CONFIG_RESOURCE_NAMESPACE,
        API_SPEC_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator,
        clientConfig);
    this.timestampConverter = timestampConverter;
    this.apiSpecStatusConverter = apiSpecStatusConverter;
  }

  public ApiSpecsResult getFilteredApiSpecsWithPaginationAndOptionalTotal(
      RequestContext requestContext,
      ApiSpecFilter apiSpecFilter,
      List<ai.traceable.api.spec.config.service.v1.SortBy> sortByList,
      Pagination pagination,
      boolean totalIncluded) {
    List<ContextualConfigObject<ApiSpec>> specsList;
    long totalCount = 0;
    org.hypertrace.config.service.v1.Pagination convertedPagination = convertPagination(pagination);
    if (totalIncluded) {
      ConfigsResponse<ContextualConfigObject<ApiSpec>> specsResult =
          getMatchingObjectsWithTotalCount(
              requestContext, apiSpecFilter, sortByList, convertedPagination);
      specsList = specsResult.getContextualConfigObjects();
      totalCount = specsResult.totalCount();
    } else {
      specsList =
          getMatchingObjects(requestContext, apiSpecFilter, sortByList, convertedPagination);
    }

    List<ApiSpec> apiSpecsList =
        specsList.stream()
            .map(
                contextualConfigObject ->
                    ApiSpec.newBuilder(contextualConfigObject.getData())
                        .setCreationTimestamp(
                            timestampConverter.convert(
                                contextualConfigObject.getCreationTimestamp()))
                        .setLastUpdatedTimestamp(
                            timestampConverter.convert(
                                contextualConfigObject.getLastUpdatedTimestamp()))
                        .setSpecType(
                            SpecType.SPEC_TYPE_UNSPECIFIED.equals(
                                    contextualConfigObject.getData().getSpecType())
                                ? SpecType.SPEC_TYPE_OPEN_API_SPEC // defaulting for backward
                                // compatibility
                                : contextualConfigObject.getData().getSpecType())
                        .setStatus(
                            this.apiSpecStatusConverter.convert(
                                contextualConfigObject.getData().getStatus()))
                        .build())
            .collect(Collectors.toUnmodifiableList());
    return ApiSpecsResult.builder().specs(apiSpecsList).totalCount(totalCount).build();
  }

  public long getTotalMatchingCount(RequestContext requestContext, ApiSpecFilter apiSpecFilter) {
    return getMatchingDataWithTotalCount(
            requestContext,
            apiSpecFilter,
            Collections.emptyList(),
            org.hypertrace.config.service.v1.Pagination.newBuilder()
                .setOffset(0)
                .setLimit(1)
                .build())
        .totalCount();
  }

  private static org.hypertrace.config.service.v1.Pagination convertPagination(
      Pagination pagination) {
    if (pagination == null || Pagination.getDefaultInstance().equals(pagination)) {
      return null;
    }
    return org.hypertrace.config.service.v1.Pagination.newBuilder()
        .setLimit(pagination.getLimit())
        .setOffset(pagination.getOffset())
        .build();
  }

  public Optional<ApiSpec> getData(RequestContext requestContext, String id) {
    return getMatchingData(
        requestContext,
        ApiSpecFilter.newBuilder().setIds(StringList.newBuilder().addValues(id).build()).build(),
        Collections.emptyList());
  }

  public Optional<ApiSpec> getData(RequestContext requestContext, ApiSpecFilter apiSpecFilter) {
    return getMatchingData(requestContext, apiSpecFilter, Collections.emptyList());
  }

  @SneakyThrows
  @Override
  protected Optional<ApiSpec> buildDataFromValue(Value value) {
    ApiSpec.Builder configBuilder = ApiSpec.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, configBuilder);
    configBuilder.setStatus(this.apiSpecStatusConverter.convert(configBuilder.getStatus()));
    return Optional.of(configBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ApiSpec apiSpec) {
    return ConfigProtoConverter.convertToValue(apiSpec);
  }

  @Override
  protected String getContextFromData(ApiSpec apiSpec) {
    return apiSpec.getSpecId();
  }

  @Override
  protected SortBy buildSort(ai.traceable.api.spec.config.service.v1.SortBy sortInput) {
    if (sortInput.hasSelection()) {
      return SortBy.newBuilder()
          .setSelection(
              org.hypertrace.config.service.v1.Selection.newBuilder()
                  .setConfigJsonPath(getJsonPath(sortInput.getSelection())))
          .setSortOrder(getSortOrder(sortInput.getSortOrder()))
          .build();
    }
    return SortBy.getDefaultInstance();
  }

  private SortOrder getSortOrder(ai.traceable.api.spec.config.service.v1.SortOrder sortOrder) {
    switch (sortOrder) {
      case SORT_ORDER_ASC:
        return SortOrder.SORT_ORDER_ASC;
      case SORT_ORDER_DESC:
      default:
        return SortOrder.SORT_ORDER_DESC;
    }
  }

  private String getJsonPath(Selection selection) {
    switch (selection.getSortableField()) {
      case SORTABLE_FIELD_CREATION_TIMESTAMP:
      default:
        return CREATION_TIMESTAMP;
    }
  }

  protected Filter buildFilter(ApiSpecFilter filterInput) {
    List<Filter> filters = new ArrayList<>();
    // Handle oneof filter
    switch (filterInput.getFilterCase()) {
      case FILE_CONTENT_SHA_256:
        filters.add(
            buildInFilter(
                FILE_CONTENT_SHA_256,
                filterInput.getFileContentSha256().getFileContentSha256List()));
        break;
      case IDS:
        filters.add(buildInFilter(SPEC_ID, filterInput.getIds().getValuesList()));
        break;
      case SPEC_PATHS:
        filters.add(buildInFilter(SPEC_PATH, filterInput.getSpecPaths().getValuesList()));
        break;
    }

    // spec_type_filter
    if (filterInput.hasSpecTypeFilter()
        && !filterInput.getSpecTypeFilter().getSpecTypesList().isEmpty()) {
      filters.add(buildInFilter(SPEC_TYPE, filterInput.getSpecTypeFilter().getSpecTypesList()));
    }

    // api_inspector_disabled
    if (filterInput.hasApiInspectorDisabled()) {
      filters.add(
          buildEqualsFilter(
              API_INSPECTOR_DISABLED,
              Value.newBuilder().setBoolValue(filterInput.getApiInspectorDisabled()).build()));
    }

    // reference_type
    if (filterInput.hasReferenceType()
        && !filterInput.getReferenceType().getReferenceTypesList().isEmpty()) {
      filters.add(
          buildInFilter(REFERENCE_TYPE, filterInput.getReferenceType().getReferenceTypesList()));
    }

    // status_filter
    if (filterInput.hasStatusFilter()
        && !filterInput.getStatusFilter().getStatusesList().isEmpty()) {
      filters.add(
          buildInFilter(
              STATUS,
              filterInput.getStatusFilter().getStatusesList().stream()
                  .map(apiSpecStatusConverter::convert)
                  .collect(Collectors.toList())));
    }

    // spec_resolution_state_filter
    if (filterInput.hasSpecResolutionStateFilter()
        && !filterInput.getSpecResolutionStateFilter().getSpecResolutionStatesList().isEmpty()) {
      filters.add(
          buildInFilter(
              OPENAPI_RESOLUTION_STATE_PATH,
              filterInput.getSpecResolutionStateFilter().getSpecResolutionStatesList()));
    }

    // names
    if (filterInput.hasNames() && !filterInput.getNames().getValuesList().isEmpty()) {
      filters.add(buildInFilter(NAME, filterInput.getNames().getValuesList()));
    }

    // reference_api_spec (build OR condition of spec_id/spec_path)
    if (filterInput.hasReferenceApiSpec()
        && !filterInput.getReferenceApiSpec().getReferenceApiSpecsList().isEmpty()) {
      List<Filter> refFilters = new ArrayList<>();
      for (ReferenceApiSpec ref : filterInput.getReferenceApiSpec().getReferenceApiSpecsList()) {
        switch (ref.getReferenceTypeCase()) {
          case SPEC_ID:
            refFilters.add(
                buildEqualsFilter(
                    REFERENCE_SPEC_COMPLETE_SPEC_ID_PATH, stringValue(ref.getSpecId())));
            refFilters.add(
                buildEqualsFilter(
                    REFERENCE_SPEC_IN_COMPLETE_SPEC_ID_PATH, stringValue(ref.getSpecId())));
            break;
          case SPEC_PATH:
            refFilters.add(
                buildEqualsFilter(
                    REFERENCE_SPEC_RESOLVED_SPEC_PATH, stringValue(ref.getSpecPath())));
            break;
        }
      }
      if (!refFilters.isEmpty()) {
        filters.add(
            Filter.newBuilder()
                .setLogicalFilter(
                    LogicalFilter.newBuilder()
                        .setOperator(LogicalOperator.LOGICAL_OPERATOR_OR)
                        .addAllOperands(refFilters))
                .build());
      }
    }

    // Rest of the method remains the same
    if (filters.isEmpty()) {
      return Filter.getDefaultInstance();
    } else if (filters.size() == 1) {
      return filters.get(0);
    } else {
      return Filter.newBuilder()
          .setLogicalFilter(
              LogicalFilter.newBuilder()
                  .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                  .addAllOperands(filters))
          .build();
    }
  }

  // Helper to create IN filter
  private Filter buildInFilter(String path, List<?> values) {
    ListValue.Builder listValue = ListValue.newBuilder();
    values.forEach(
        v -> listValue.addValues(Value.newBuilder().setStringValue(v.toString()).build()));

    return Filter.newBuilder()
        .setRelationalFilter(
            RelationalFilter.newBuilder()
                .setConfigJsonPath(path)
                .setOperator(RelationalOperator.RELATIONAL_OPERATOR_IN)
                .setValue(Value.newBuilder().setListValue(listValue.build()).build()))
        .build();
  }

  // Helper to create EQ filter
  private Filter buildEqualsFilter(String path, Value value) {
    return Filter.newBuilder()
        .setRelationalFilter(
            RelationalFilter.newBuilder()
                .setConfigJsonPath(path)
                .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                .setValue(value))
        .build();
  }

  // Helper for string values
  private Value stringValue(String s) {
    return Value.newBuilder().setStringValue(s).build();
  }
}
