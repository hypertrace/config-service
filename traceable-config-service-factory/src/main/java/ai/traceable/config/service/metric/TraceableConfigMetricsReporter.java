package ai.traceable.config.service.metric;

import static ai.traceable.config.service.metric.FilterSupplier.getConfigsMatchingResourceNameFilter;
import static ai.traceable.config.service.metric.FilterSupplier.getSegmentMatchingBasedApiNamingRulesFilter;
import static ai.traceable.config.service.metric.FilterSupplier.getSpecBasedApiNamingRulesApiSpecIdsFilter;
import static ai.traceable.config.service.metric.FilterSupplier.getSpecBasedApiNamingRulesFilter;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.model.config.CustomMetricConfig.VALUE_KEY;

import java.time.Duration;
import java.util.List;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.model.config.CustomMetricConfig;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.serviceframework.docstore.metrics.DocStoreCustomMetricReportingConfig;

public class TraceableConfigMetricsReporter {
  private static final String API_DOCUMENTATION_COUNT_METRIC_NAME = "api.documentation.count";
  private static final String API_NAMING_RULES_PER_SPEC_COUNT_METRIC_NAME =
      "api.naming.rules.per.spec.count";
  private static final String SPEC_BASED_API_NAMING_RULES_COUNT_METRIC_NAME =
      "spec.based.api.naming.rules.count";
  private static final String SEGMENT_MATCHING_BASED_API_NAMING_RULES_COUNT_METRIC_NAME =
      "segment.matching.based.api.naming.rules.count";

  private static final String API_SPEC_RESOURCE_NAME = "api-spec";
  private static final String CONFIGURATIONS_COLLECTION = "configurations";
  private static final String TENANT_ID_ENTITY_PATH = "tenantId";
  private static final String API_SPEC_SPEC_TYPE_PATH = "config.specType";
  private static final String API_SPEC_STATUS_PATH = "config.status";
  private static final String API_SPEC_API_NAMING_ENABLED_PATH = "config.apiNamingEnabled";
  private static final String API_SPEC_API_INSPECTOR_DISABLED_PATH = "config.apiInspectorDisabled";
  private static final String API_SPEC_REFERENCE_TYPE_PATH = "config.referenceType";
  private static final String API_SPEC_BASED_API_NAMING_RULE_API_SPEC_IDS_PATH =
      "config.ruleInfo.ruleConfig.apiSpecBasedConfig.apiSpecIds";
  private static final String API_NAMING_RULES_DISABLED_PATH = "config.ruleInfo.disabled";

  private static final String API_SPEC_SPEC_TYPE = "specType";
  private static final String API_SPEC_STATUS = "status";
  private static final String API_SPEC_API_NAMING_ENABLED = "apiNamingEnabled";
  private static final String API_SPEC_API_INSPECTOR_DISABLED = "apiInspectorDisabled";
  private static final String API_SPEC_REFERENCE_TYPE = "referenceType";
  private static final String API_SPEC_ID = "apiSpecId";
  private static final String API_NAMING_RULES_DISABLED = "disabled";

  public static List<DocStoreCustomMetricReportingConfig> getConfigurationCounterConfig() {
    return List.of(
        DocStoreCustomMetricReportingConfig.builder()
            .reportingInterval(Duration.ofHours(6))
            .config(
                CustomMetricConfig.builder()
                    .metricName(API_DOCUMENTATION_COUNT_METRIC_NAME)
                    .collectionName(CONFIGURATIONS_COLLECTION)
                    .query(
                        Query.builder()
                            .setFilter(getConfigsMatchingResourceNameFilter(API_SPEC_RESOURCE_NAME))
                            .addSelection(
                                IdentifierExpression.of(TENANT_ID_ENTITY_PATH),
                                TENANT_ID_ENTITY_PATH)
                            .addSelection(
                                AggregateExpression.of(COUNT, ConstantExpression.of(1)), VALUE_KEY)
                            .addSelection(
                                IdentifierExpression.of(API_SPEC_SPEC_TYPE_PATH),
                                API_SPEC_SPEC_TYPE)
                            .addSelection(
                                IdentifierExpression.of(API_SPEC_STATUS_PATH), API_SPEC_STATUS)
                            .addSelection(
                                IdentifierExpression.of(API_SPEC_API_NAMING_ENABLED_PATH),
                                API_SPEC_API_NAMING_ENABLED)
                            .addSelection(
                                IdentifierExpression.of(API_SPEC_API_INSPECTOR_DISABLED_PATH),
                                API_SPEC_API_INSPECTOR_DISABLED)
                            .addSelection(
                                IdentifierExpression.of(API_SPEC_REFERENCE_TYPE_PATH),
                                API_SPEC_REFERENCE_TYPE)
                            .addAggregation(IdentifierExpression.of(TENANT_ID_ENTITY_PATH))
                            .addAggregation(IdentifierExpression.of(API_SPEC_SPEC_TYPE_PATH))
                            .addAggregation(IdentifierExpression.of(API_SPEC_STATUS_PATH))
                            .addAggregation(
                                IdentifierExpression.of(API_SPEC_API_NAMING_ENABLED_PATH))
                            .addAggregation(
                                IdentifierExpression.of(API_SPEC_API_INSPECTOR_DISABLED_PATH))
                            .addAggregation(IdentifierExpression.of(API_SPEC_REFERENCE_TYPE_PATH))
                            .build())
                    .build())
            .build(),
        DocStoreCustomMetricReportingConfig.builder()
            .reportingInterval(Duration.ofHours(6))
            .config(
                CustomMetricConfig.builder()
                    .metricName(SPEC_BASED_API_NAMING_RULES_COUNT_METRIC_NAME)
                    .collectionName(CONFIGURATIONS_COLLECTION)
                    .query(
                        Query.builder()
                            .setFilter(getSpecBasedApiNamingRulesFilter())
                            .addSelection(
                                IdentifierExpression.of(TENANT_ID_ENTITY_PATH),
                                TENANT_ID_ENTITY_PATH)
                            .addSelection(
                                AggregateExpression.of(COUNT, ConstantExpression.of(1)), VALUE_KEY)
                            .addSelection(
                                IdentifierExpression.of(API_NAMING_RULES_DISABLED_PATH),
                                API_NAMING_RULES_DISABLED)
                            .addAggregation(IdentifierExpression.of(TENANT_ID_ENTITY_PATH))
                            .addAggregation(IdentifierExpression.of(API_NAMING_RULES_DISABLED_PATH))
                            .build())
                    .build())
            .build(),
        DocStoreCustomMetricReportingConfig.builder()
            .reportingInterval(Duration.ofHours(6))
            .config(
                CustomMetricConfig.builder()
                    .metricName(SEGMENT_MATCHING_BASED_API_NAMING_RULES_COUNT_METRIC_NAME)
                    .collectionName(CONFIGURATIONS_COLLECTION)
                    .query(
                        Query.builder()
                            .setFilter(getSegmentMatchingBasedApiNamingRulesFilter())
                            .addSelection(
                                IdentifierExpression.of(TENANT_ID_ENTITY_PATH),
                                TENANT_ID_ENTITY_PATH)
                            .addSelection(
                                AggregateExpression.of(COUNT, ConstantExpression.of(1)), VALUE_KEY)
                            .addSelection(
                                IdentifierExpression.of(API_NAMING_RULES_DISABLED_PATH),
                                API_NAMING_RULES_DISABLED)
                            .addAggregation(IdentifierExpression.of(TENANT_ID_ENTITY_PATH))
                            .addAggregation(IdentifierExpression.of(API_NAMING_RULES_DISABLED_PATH))
                            .build())
                    .build())
            .build(),
        DocStoreCustomMetricReportingConfig.builder()
            .reportingInterval(Duration.ofHours(6))
            .config(
                CustomMetricConfig.builder()
                    .metricName(API_NAMING_RULES_PER_SPEC_COUNT_METRIC_NAME)
                    .collectionName(CONFIGURATIONS_COLLECTION)
                    .query(
                        Query.builder()
                            .setFilter(getSpecBasedApiNamingRulesApiSpecIdsFilter())
                            .addFromClause(
                                UnnestExpression.builder()
                                    .identifierExpression(
                                        IdentifierExpression.of(
                                            API_SPEC_BASED_API_NAMING_RULE_API_SPEC_IDS_PATH))
                                    .build())
                            .addSelection(
                                IdentifierExpression.of(TENANT_ID_ENTITY_PATH),
                                TENANT_ID_ENTITY_PATH)
                            .addSelection(
                                AggregateExpression.of(COUNT, ConstantExpression.of(1)), VALUE_KEY)
                            .addSelection(
                                IdentifierExpression.of(
                                    API_SPEC_BASED_API_NAMING_RULE_API_SPEC_IDS_PATH),
                                API_SPEC_ID)
                            .addSelection(
                                IdentifierExpression.of(API_NAMING_RULES_DISABLED_PATH),
                                API_NAMING_RULES_DISABLED)
                            .addAggregation(IdentifierExpression.of(TENANT_ID_ENTITY_PATH))
                            .addAggregation(
                                IdentifierExpression.of(
                                    API_SPEC_BASED_API_NAMING_RULE_API_SPEC_IDS_PATH))
                            .addAggregation(IdentifierExpression.of(API_NAMING_RULES_DISABLED_PATH))
                            .build())
                    .build())
            .build());
  }
}
