package ai.traceable.span.processing.config.service.store;

import static ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig.RuleConfigCase.API_SPEC_BASED_CONFIG;
import static ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig.RuleConfigCase.AST_SCAN_BASED_CONFIG;
import static ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig.RuleConfigCase.GEN_AI_BASED_CONFIG;
import static ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig.RuleConfigCase.SEGMENT_MATCHING_BASED_CONFIG;
import static ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfigType.API_NAMING_RULE_CONFIG_TYPE_API_SPEC_BASED_CONFIG;
import static ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfigType.API_NAMING_RULE_CONFIG_TYPE_AST_SCAN_BASED_CONFIG;
import static ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfigType.API_NAMING_RULE_CONFIG_TYPE_GEN_AI_BASED_CONFIG;
import static ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfigType.API_NAMING_RULE_CONFIG_TYPE_SEGMENT_MATCHING_BASED_CONFIG;
import static ai.traceable.span.processing.config.service.v1.Field.FIELD_ENVIRONMENT_NAME;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.span.processing.config.service.SpanProcessingConfigConstants;
import ai.traceable.span.processing.config.service.v1.ApiNamingRule;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig.RuleConfigCase;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfigType;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleDetails;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleMetadata;
import ai.traceable.span.processing.config.service.v1.ApiNamingRulePagination;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleSelection;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleSortBy;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleSortOrder;
import ai.traceable.span.processing.config.service.v1.ApiNamingRulesFilter;
import ai.traceable.span.processing.config.service.v1.ScopeFilter;
import com.google.common.collect.ImmutableBiMap;
import com.google.inject.Inject;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import io.grpc.Status;
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
import org.hypertrace.config.service.v1.Selection;
import org.hypertrace.config.service.v1.SortBy;
import org.hypertrace.config.service.v1.SortOrder;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ApiNamingRulesConfigStore
    extends IdentifiedFilterPushedDownObjectStore<
        ApiNamingRule, ApiNamingRulesFilter, ApiNamingRuleSortBy> {

  private static final String API_NAMING_RULES_RESOURCE_NAME = "api-naming-rules";

  private static final String ID = "id";
  private static final String CREATION_TIMESTAMP = "creationTimestamp";
  private static final String DISABLED = "ruleInfo.disabled";
  private static final String RULE_CONFIG = "ruleInfo.ruleConfig.";
  private static final String RULE_CONFIG_API_SPEC_IDS =
      "ruleInfo.ruleConfig.apiSpecBasedConfig.apiSpecIds";
  private static final String DEPLOYMENT_ENVIRONMENT_ATTRIBUTE_KEY = "deployment.environment";
  private static final ImmutableBiMap<ApiNamingRuleConfigType, RuleConfigCase>
      RULE_CONFIG_TYPE_RULE_CONFIG_CASE_BI_MAP =
          ImmutableBiMap.<ApiNamingRuleConfigType, RuleConfigCase>builder()
              .put(
                  API_NAMING_RULE_CONFIG_TYPE_SEGMENT_MATCHING_BASED_CONFIG,
                  SEGMENT_MATCHING_BASED_CONFIG)
              .put(API_NAMING_RULE_CONFIG_TYPE_API_SPEC_BASED_CONFIG, API_SPEC_BASED_CONFIG)
              .put(API_NAMING_RULE_CONFIG_TYPE_AST_SCAN_BASED_CONFIG, AST_SCAN_BASED_CONFIG)
              .put(API_NAMING_RULE_CONFIG_TYPE_GEN_AI_BASED_CONFIG, GEN_AI_BASED_CONFIG)
              .build();

  private final TimestampConverter timestampConverter;

  @Inject
  public ApiNamingRulesConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      TimestampConverter timestampConverter,
      ConfigChangeEventGenerator configChangeEventGenerator,
      ClientConfig clientConfig) {
    super(
        configServiceBlockingStub,
        SpanProcessingConfigConstants.RESOURCE_NAMESPACE,
        API_NAMING_RULES_RESOURCE_NAME,
        configChangeEventGenerator,
        clientConfig);
    this.timestampConverter = timestampConverter;
  }

  public List<ApiNamingRuleDetails> getAllRuleDetails(RequestContext requestContext) {
    return this.getRuleDetails(requestContext, ApiNamingRulesFilter.getDefaultInstance());
  }

  public List<ApiNamingRuleDetails> getRuleDetails(
      RequestContext requestContext, ApiNamingRulesFilter apiNamingRulesFilter) {
    List<ContextualConfigObject<ApiNamingRule>> rulesList =
        this.getMatchingObjects(
            requestContext, apiNamingRulesFilter, Collections.emptyList(), null);

    // Apply scope filter in-memory as post-processing (cannot be pushed down to database)
    if (apiNamingRulesFilter.hasScopeFilter()) {
      rulesList =
          rulesList.stream()
              .filter(
                  contextualConfigObject ->
                      matchesScopeFilter(
                          contextualConfigObject.getData(), apiNamingRulesFilter.getScopeFilter()))
              .collect(Collectors.toList());
    }

    return rulesList.stream()
        .map(
            contextualConfigObject ->
                ApiNamingRuleDetails.newBuilder()
                    .setRule(contextualConfigObject.getData())
                    .setMetadata(
                        ApiNamingRuleMetadata.newBuilder()
                            .setCreationTimestamp(
                                timestampConverter.convert(
                                    contextualConfigObject.getCreationTimestamp()))
                            .setLastUpdatedTimestamp(
                                timestampConverter.convert(
                                    contextualConfigObject.getLastUpdatedTimestamp()))
                            .build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  @Override
  protected Optional<ApiNamingRule> buildDataFromValue(Value value) {
    ApiNamingRule.Builder ruleBuilder = ApiNamingRule.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, ruleBuilder);
    return Optional.of(ruleBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ApiNamingRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(ApiNamingRule rule) {
    return rule.getId();
  }

  @Override
  protected Filter buildFilter(ApiNamingRulesFilter filterInput) {
    final List<Filter> filters = new ArrayList<>();

    if (!filterInput.getIdsList().isEmpty()) {
      filters.add(buildInFilter(ID, filterInput.getIdsList()));
    }

    if (!filterInput.getApiSpecIdsList().isEmpty()) {
      filters.add(buildInFilter(RULE_CONFIG_API_SPEC_IDS, filterInput.getApiSpecIdsList()));
    }

    buildDisabledFilter(filterInput).ifPresent(filters::add);

    if (!filterInput.getRuleConfigTypesList().isEmpty()) {
      filters.add(buildRuleConfigTypesFilter(filterInput.getRuleConfigTypesList()));
    }

    return buildAndFilter(filters);
  }

  @Override
  protected SortBy buildSort(ApiNamingRuleSortBy sortInput) {
    if (sortInput.hasSelection()) {
      SortBy build =
          SortBy.newBuilder()
              .setSelection(
                  Selection.newBuilder().setConfigJsonPath(getJsonPath(sortInput.getSelection())))
              .setSortOrder(getSortOrder(sortInput.getSortOrder()))
              .build();
      return build;
    }
    return SortBy.getDefaultInstance();
  }

  private RuleConfigCase convertToRuleConfigCase(ApiNamingRuleConfigType apiNamingRuleConfigType) {
    return Optional.ofNullable(
            RULE_CONFIG_TYPE_RULE_CONFIG_CASE_BI_MAP.get(apiNamingRuleConfigType))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Unsupported rule config type: " + apiNamingRuleConfigType));
  }

  private String getRuleConfigFieldName(RuleConfigCase ruleConfigCase) {
    switch (ruleConfigCase) {
      case SEGMENT_MATCHING_BASED_CONFIG:
        return "segmentMatchingBasedConfig";
      case API_SPEC_BASED_CONFIG:
        return "apiSpecBasedConfig";
      case AST_SCAN_BASED_CONFIG:
        return "astScanBasedConfig";
      case GEN_AI_BASED_CONFIG:
        return "genAiBasedConfig";
      case JOB_BASED_CONFIG:
        return "jobBasedConfig";
      default:
        throw new IllegalArgumentException("Unsupported rule config case: " + ruleConfigCase);
    }
  }

  private Filter buildInFilter(String path, List<?> values) {
    final ListValue.Builder listValue = ListValue.newBuilder();
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

  private Filter buildEqualsFilter(String path, Value value) {
    return Filter.newBuilder()
        .setRelationalFilter(
            RelationalFilter.newBuilder()
                .setConfigJsonPath(path)
                .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                .setValue(value))
        .build();
  }

  private Filter buildExistsFilter(String path) {
    return Filter.newBuilder()
        .setRelationalFilter(
            RelationalFilter.newBuilder()
                .setConfigJsonPath(path)
                .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EXISTS)
                .setValue(Value.newBuilder().setBoolValue(true)))
        .build();
  }

  private Filter buildNotExistsFilter(String path) {
    return Filter.newBuilder()
        .setRelationalFilter(
            RelationalFilter.newBuilder()
                .setConfigJsonPath(path)
                .setOperator(RelationalOperator.RELATIONAL_OPERATOR_NOT_EXISTS))
        .build();
  }

  private Filter buildOrFilter(List<Filter> filters) {
    if (filters.isEmpty()) {
      return Filter.getDefaultInstance();
    } else if (filters.size() == 1) {
      return filters.get(0);
    } else {
      return Filter.newBuilder()
          .setLogicalFilter(
              LogicalFilter.newBuilder()
                  .setOperator(LogicalOperator.LOGICAL_OPERATOR_OR)
                  .addAllOperands(filters))
          .build();
    }
  }

  private Filter buildAndFilter(List<Filter> filters) {
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

  private Optional<Filter> buildDisabledFilter(ApiNamingRulesFilter filterInput) {
    if (filterInput.hasDisabled()) {
      if (filterInput.getDisabled()) {
        return Optional.of(
            buildEqualsFilter(DISABLED, Value.newBuilder().setBoolValue(true).build()));
      } else {
        return Optional.of(buildNotExistsFilter(DISABLED));
      }
    }
    return Optional.empty();
  }

  private Filter buildRuleConfigTypesFilter(List<ApiNamingRuleConfigType> ruleConfigTypes) {
    final List<Filter> ruleConfigFilters = new ArrayList<>();
    for (final ApiNamingRuleConfigType configType : ruleConfigTypes) {
      final RuleConfigCase ruleConfigCase = convertToRuleConfigCase(configType);
      ruleConfigFilters.add(
          buildExistsFilter(RULE_CONFIG + getRuleConfigFieldName(ruleConfigCase)));
    }
    return buildOrFilter(ruleConfigFilters);
  }

  private SortOrder getSortOrder(ApiNamingRuleSortOrder sortOrder) {
    switch (sortOrder) {
      case API_NAMING_RULE_SORT_ORDER_ASC:
        return SortOrder.SORT_ORDER_ASC;
      case API_NAMING_RULE_SORT_ORDER_DESC:
      default:
        return SortOrder.SORT_ORDER_DESC;
    }
  }

  private String getJsonPath(ApiNamingRuleSelection selection) {
    switch (selection.getSortableField()) {
      case API_NAMING_RULE_SORTABLE_FIELD_ID:
        return ID;
      default:
        return CREATION_TIMESTAMP;
    }
  }

  private org.hypertrace.config.service.v1.Pagination convertPagination(
      ApiNamingRulePagination pagination) {
    if (pagination == null || ApiNamingRulePagination.getDefaultInstance().equals(pagination)) {
      return null;
    }
    return org.hypertrace.config.service.v1.Pagination.newBuilder()
        .setLimit(pagination.getLimit())
        .setOffset(pagination.getOffset())
        .build();
  }

  public ApiNamingRulesResult getRuleDetailsWithPaginationAndOptionalTotal(
      RequestContext requestContext,
      ApiNamingRulesFilter apiNamingRulesFilter,
      List<ApiNamingRuleSortBy> sortByList,
      ApiNamingRulePagination pagination,
      boolean totalIncluded) {
    List<ContextualConfigObject<ApiNamingRule>> rulesList;
    long totalCount = 0;
    final org.hypertrace.config.service.v1.Pagination convertedPagination =
        convertPagination(pagination);

    if (totalIncluded) {
      final ConfigsResponse<ContextualConfigObject<ApiNamingRule>> rulesResult =
          getMatchingObjectsWithTotalCount(
              requestContext, apiNamingRulesFilter, sortByList, convertedPagination);
      rulesList = rulesResult.getContextualConfigObjects();
      totalCount = rulesResult.totalCount();
    } else {
      rulesList =
          getMatchingObjects(requestContext, apiNamingRulesFilter, sortByList, convertedPagination);
    }

    // Apply scope filter in-memory as post-processing (cannot be pushed down to database)
    if (apiNamingRulesFilter.hasScopeFilter()) {
      rulesList =
          rulesList.stream()
              .filter(
                  contextualConfigObject ->
                      matchesScopeFilter(
                          contextualConfigObject.getData(), apiNamingRulesFilter.getScopeFilter()))
              .collect(Collectors.toList());
      // Update total count if we filtered in-memory
      if (totalIncluded) {
        totalCount = rulesList.size();
      }
    }

    final List<ApiNamingRuleDetails> apiNamingRuleDetailsList =
        rulesList.stream()
            .map(
                contextualConfigObject ->
                    ApiNamingRuleDetails.newBuilder()
                        .setRule(contextualConfigObject.getData())
                        .setMetadata(
                            ApiNamingRuleMetadata.newBuilder()
                                .setCreationTimestamp(
                                    timestampConverter.convert(
                                        contextualConfigObject.getCreationTimestamp()))
                                .setLastUpdatedTimestamp(
                                    timestampConverter.convert(
                                        contextualConfigObject.getLastUpdatedTimestamp()))
                                .build())
                        .build())
            .collect(Collectors.toUnmodifiableList());

    return ApiNamingRulesResult.builder()
        .ruleDetails(apiNamingRuleDetailsList)
        .totalCount(totalCount)
        .build();
  }

  private boolean matchesScopeFilter(ApiNamingRule rule, ScopeFilter scopeFilter) {
    switch (scopeFilter.getScopeCase()) {
      case ENVIRONMENT_SCOPE:
        final List<String> environmentIds =
            scopeFilter.getEnvironmentScope().getEnvironmentIdsList();
        return environmentIds.isEmpty()
            || environmentIds.stream()
                .anyMatch(
                    environmentId ->
                        isEnvironmentInSpanFilter(rule.getRuleInfo().getFilter(), environmentId));
      case SCOPE_NOT_SET:
        return true;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format("Unsupported scope filter: %s", scopeFilter.getScopeCase()))
            .asRuntimeException();
    }
  }

  private boolean isEnvironmentInSpanFilter(
      ai.traceable.span.processing.config.service.v1.SpanFilter spanFilter, String environmentId) {
    switch (spanFilter.getSpanFilterExpressionCase()) {
      case RELATIONAL_SPAN_FILTER:
        return isEnvironmentInRelationalFilter(spanFilter.getRelationalSpanFilter(), environmentId);
      case LOGICAL_SPAN_FILTER:
        return spanFilter.getLogicalSpanFilter().getOperandsList().stream()
            .anyMatch(operand -> isEnvironmentInSpanFilter(operand, environmentId));
      case SPANFILTEREXPRESSION_NOT_SET:
        return false;
      default:
        return false;
    }
  }

  private boolean isEnvironmentInRelationalFilter(
      ai.traceable.span.processing.config.service.v1.RelationalSpanFilterExpression
          relationalFilter,
      String environmentId) {
    // Check if this is an environment filter
    final boolean isEnvironmentFilter =
        (relationalFilter.hasField() && relationalFilter.getField().equals(FIELD_ENVIRONMENT_NAME))
            || (relationalFilter.hasSpanAttributeKey()
                && relationalFilter
                    .getSpanAttributeKey()
                    .equals(DEPLOYMENT_ENVIRONMENT_ATTRIBUTE_KEY));

    if (!isEnvironmentFilter) {
      return false;
    }

    // Check if the environment ID matches the filter value
    final ai.traceable.span.processing.config.service.v1.SpanFilterValue rightOperand =
        relationalFilter.getRightOperand();
    switch (rightOperand.getValueCase()) {
      case STRING_VALUE:
        return rightOperand.getStringValue().equals(environmentId);
      case LIST_VALUE:
        return rightOperand.getListValue().getValuesList().stream()
            .anyMatch(
                value -> value.hasStringValue() && value.getStringValue().equals(environmentId));
      default:
        return false;
    }
  }
}
