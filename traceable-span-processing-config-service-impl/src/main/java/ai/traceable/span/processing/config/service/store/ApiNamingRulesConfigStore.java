package ai.traceable.span.processing.config.service.store;

import static ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig.RuleConfigCase.API_SPEC_BASED_CONFIG;
import static ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig.RuleConfigCase.AST_SCAN_BASED_CONFIG;
import static ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig.RuleConfigCase.SEGMENT_MATCHING_BASED_CONFIG;
import static ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfigType.API_NAMING_RULE_CONFIG_TYPE_API_SPEC_BASED_CONFIG;
import static ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfigType.API_NAMING_RULE_CONFIG_TYPE_AST_SCAN_BASED_CONFIG;
import static ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfigType.API_NAMING_RULE_CONFIG_TYPE_SEGMENT_MATCHING_BASED_CONFIG;
import static ai.traceable.span.processing.config.service.v1.Field.FIELD_ENVIRONMENT_NAME;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.span.processing.config.service.SpanProcessingConfigConstants;
import ai.traceable.span.processing.config.service.v1.ApiNamingRule;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig.RuleConfigCase;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfigType;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleDetails;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleMetadata;
import ai.traceable.span.processing.config.service.v1.ApiNamingRulesFilter;
import ai.traceable.span.processing.config.service.v1.Field;
import ai.traceable.span.processing.config.service.v1.LogicalSpanFilterExpression;
import ai.traceable.span.processing.config.service.v1.RelationalSpanFilterExpression;
import ai.traceable.span.processing.config.service.v1.ScopeFilter;
import ai.traceable.span.processing.config.service.v1.SpanFilter;
import ai.traceable.span.processing.config.service.v1.SpanFilterValue;
import com.google.common.collect.ImmutableBiMap;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ApiNamingRulesConfigStore
    extends IdentifiedObjectStoreWithFilter<ApiNamingRule, ApiNamingRulesFilter> {

  private static final String API_NAMING_RULES_RESOURCE_NAME = "api-naming-rules";
  private static final String DEPLOYMENT_ENVIRONMENT_ATTRIBUTE_KEY = "deployment.environment";
  private static final ImmutableBiMap<ApiNamingRuleConfigType, RuleConfigCase>
      RULE_CONFIG_TYPE_RULE_CONFIG_CASE_BI_MAP =
          ImmutableBiMap.<ApiNamingRuleConfigType, RuleConfigCase>builder()
              .put(
                  API_NAMING_RULE_CONFIG_TYPE_SEGMENT_MATCHING_BASED_CONFIG,
                  SEGMENT_MATCHING_BASED_CONFIG)
              .put(API_NAMING_RULE_CONFIG_TYPE_API_SPEC_BASED_CONFIG, API_SPEC_BASED_CONFIG)
              .put(API_NAMING_RULE_CONFIG_TYPE_AST_SCAN_BASED_CONFIG, AST_SCAN_BASED_CONFIG)
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
    return this.getAllObjects(requestContext, apiNamingRulesFilter).stream()
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
  protected Optional<ApiNamingRule> filterConfigData(
      ApiNamingRule apiNamingRule, ApiNamingRulesFilter filter) {
    return Optional.of(apiNamingRule).filter(rule -> this.matchFilter(rule, filter));
  }

  private boolean matchFilter(ApiNamingRule rule, ApiNamingRulesFilter filter) {
    return matchIds(rule, filter)
        && matchApiSpecIds(rule, filter)
        && matchDisabled(rule, filter)
        && matchRuleConfigType(rule, filter)
        && matchScopeFilters(rule, filter);
  }

  private boolean matchIds(ApiNamingRule rule, ApiNamingRulesFilter filter) {
    return filter.getIdsList().isEmpty() || filter.getIdsList().contains(rule.getId());
  }

  private boolean matchApiSpecIds(ApiNamingRule rule, ApiNamingRulesFilter filter) {
    return filter.getApiSpecIdsList().isEmpty()
        || findAnyMatchingApiSpecIds(rule, filter.getApiSpecIdsList());
  }

  private boolean findAnyMatchingApiSpecIds(ApiNamingRule rule, List<String> apiSpecIds) {
    // find any api spec id in filter that matches spec ids specified in the rule
    return rule.getRuleInfo().getRuleConfig().hasApiSpecBasedConfig()
        && rule.getRuleInfo().getRuleConfig().getApiSpecBasedConfig().getApiSpecIdsList().stream()
            .anyMatch(apiSpecIds::contains);
  }

  private boolean matchDisabled(ApiNamingRule rule, ApiNamingRulesFilter filter) {
    return !filter.hasDisabled() || filter.getDisabled() == rule.getRuleInfo().getDisabled();
  }

  private boolean matchRuleConfigType(ApiNamingRule rule, ApiNamingRulesFilter filter) {
    return filter.getRuleConfigTypesList().isEmpty()
        || filter.getRuleConfigTypesList().stream()
            .map(this::convert)
            .anyMatch(rule.getRuleInfo().getRuleConfig().getRuleConfigCase()::equals);
  }

  private RuleConfigCase convert(ApiNamingRuleConfigType apiNamingRuleConfigType) {
    return Optional.ofNullable(
            RULE_CONFIG_TYPE_RULE_CONFIG_CASE_BI_MAP.get(apiNamingRuleConfigType))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Unsupported rule config type: " + apiNamingRuleConfigType));
  }

  private boolean matchScopeFilters(ApiNamingRule rule, ApiNamingRulesFilter filter) {
    return !filter.hasScopeFilter()
        || matchScopeFilter(rule.getRuleInfo().getFilter(), filter.getScopeFilter());
  }

  private boolean matchScopeFilter(SpanFilter spanFilter, ScopeFilter scopeFilter) {
    switch (scopeFilter.getScopeCase()) {
      case ENVIRONMENT_SCOPE:
        List<String> environmentNames =
            scopeFilter.getEnvironmentScope().getEnvironmentIdsList().stream()
                .map(this::getEnvironmentNameForId)
                .collect(Collectors.toUnmodifiableList());
        return environmentNames.isEmpty()
            || environmentNames.stream()
                .anyMatch(
                    environmentName ->
                        isAttributePartOfSpanFilter(
                            spanFilter, buildSpanAttributeKeyForEnvironment(), environmentName));
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format("Unsupported scope filter: %s", scopeFilter.getScopeCase()))
            .asRuntimeException();
    }
  }

  private String getEnvironmentNameForId(String environmentId) {
    return environmentId;
  }

  private SpanAttributeKey buildSpanAttributeKeyForEnvironment() {
    return SpanAttributeKey.builder()
        .field(FIELD_ENVIRONMENT_NAME)
        .key(DEPLOYMENT_ENVIRONMENT_ATTRIBUTE_KEY)
        .build();
  }

  private boolean isAttributePartOfSpanFilter(
      SpanFilter spanFilter, SpanAttributeKey spanAttributeKey, String attributeValue) {
    switch (spanFilter.getSpanFilterExpressionCase()) {
      case RELATIONAL_SPAN_FILTER:
        return isAttributePartOfRelationalFilter(
            spanFilter.getRelationalSpanFilter(), spanAttributeKey, attributeValue);
      case LOGICAL_SPAN_FILTER:
        return isAttributePartOfLogicalFilter(
            spanFilter.getLogicalSpanFilter(), spanAttributeKey, attributeValue);
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Unsupported span filter expression case: %s",
                    spanFilter.getSpanFilterExpressionCase()))
            .asRuntimeException();
    }
  }

  private boolean isAttributePartOfRelationalFilter(
      RelationalSpanFilterExpression relationalSpanFilter,
      SpanAttributeKey spanAttributeKey,
      String attributeValue) {
    switch (relationalSpanFilter.getLeftOperandCase()) {
      case FIELD:
        if (!relationalSpanFilter.getField().equals(spanAttributeKey.getField())) {
          return false;
        }
        break;
      case SPAN_ATTRIBUTE_KEY:
        if (!relationalSpanFilter.getSpanAttributeKey().equals(spanAttributeKey.getKey())) {
          return false;
        }
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid left operand case: %s", relationalSpanFilter.getLeftOperandCase()))
            .asRuntimeException();
    }

    List<String> rhsValues = extractValues(relationalSpanFilter.getRightOperand());
    switch (relationalSpanFilter.getOperator()) {
      case RELATIONAL_OPERATOR_EQUALS:
        return rhsValues.size() == 1 && rhsValues.contains(attributeValue);
      case RELATIONAL_OPERATOR_IN:
        return rhsValues.stream().allMatch(attributeValue::equals);
      default:
        return false;
    }
  }

  private boolean isAttributePartOfLogicalFilter(
      LogicalSpanFilterExpression logicalSpanFilter,
      SpanAttributeKey spanAttributeKey,
      String attributeValue) {
    switch (logicalSpanFilter.getOperator()) {
      case LOGICAL_OPERATOR_AND:
        // anyMatch because goal is to find at least one span filter condition that matches the
        // condition
        return logicalSpanFilter.getOperandsList().stream()
            .anyMatch(
                operand -> isAttributePartOfSpanFilter(operand, spanAttributeKey, attributeValue));
      default:
        return false;
    }
  }

  private List<String> extractValues(SpanFilterValue spanFilterValue) {
    switch (spanFilterValue.getValueCase()) {
      case STRING_VALUE:
        return List.of(spanFilterValue.getStringValue());
      case LIST_VALUE:
        return spanFilterValue.getListValue().getValuesList().stream()
            .map(this::extractValues)
            .flatMap(List::stream)
            .collect(Collectors.toUnmodifiableList());
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Unsupported span filter value case: %s", spanFilterValue.getValueCase()))
            .asRuntimeException();
    }
  }

  @Builder
  @lombok.Value
  private static class SpanAttributeKey {
    @Nullable String key;
    @Nullable Field field;
  }
}
