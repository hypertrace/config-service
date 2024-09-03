package ai.traceable.config.service.metric;

import java.util.List;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.query.Filter;

public class FilterSupplier {
  private static final String API_NAMING_RULES_RESOURCE_NAME = "api-naming-rules";
  private static final String CONFIG_PATH = "config";
  private static final String RESOURCE_NAME_PATH = "resourceName";
  private static final String SEGMENT_MATCHING_BASED_API_NAMING_RULE_CONFIG_PATH =
      "config.ruleInfo.ruleConfig.segmentMatchingBasedConfig";
  private static final String API_SPEC_BASED_API_NAMING_CONFIG_PATH =
      "config.ruleInfo.ruleConfig.apiSpecBasedConfig";
  private static final String API_SPEC_BASED_API_NAMING_RULE_API_SPEC_IDS_PATH =
      API_SPEC_BASED_API_NAMING_CONFIG_PATH + ".apiSpecIds";

  public static Filter getConfigsMatchingResourceNameFilter(String resourceName) {
    return Filter.builder()
        .expression(
            LogicalExpression.and(
                List.of(
                    getResourceNameRelationalFilter(resourceName),
                    getConfigNonNullRelationalFilter())))
        .build();
  }

  public static Filter getSpecBasedApiNamingRulesFilter() {
    return Filter.builder()
        .expression(
            LogicalExpression.and(
                List.of(
                    getResourceNameRelationalFilter(API_NAMING_RULES_RESOURCE_NAME),
                    getConfigNonNullRelationalFilter(),
                    getConfigPathExistsRelationalFilter(API_SPEC_BASED_API_NAMING_CONFIG_PATH))))
        .build();
  }

  public static Filter getSegmentMatchingBasedApiNamingRulesFilter() {
    return Filter.builder()
        .expression(
            LogicalExpression.and(
                List.of(
                    getResourceNameRelationalFilter(API_NAMING_RULES_RESOURCE_NAME),
                    getConfigNonNullRelationalFilter(),
                    getConfigPathExistsRelationalFilter(
                        SEGMENT_MATCHING_BASED_API_NAMING_RULE_CONFIG_PATH))))
        .build();
  }

  public static Filter getSpecBasedApiNamingRulesApiSpecIdsFilter() {
    return Filter.builder()
        .expression(
            LogicalExpression.and(
                List.of(
                    getResourceNameRelationalFilter(API_NAMING_RULES_RESOURCE_NAME),
                    getConfigNonNullRelationalFilter(),
                    getConfigPathExistsRelationalFilter(
                        API_SPEC_BASED_API_NAMING_RULE_API_SPEC_IDS_PATH))))
        .build();
  }

  private static RelationalExpression getResourceNameRelationalFilter(String resourceName) {
    return RelationalExpression.of(
        IdentifierExpression.of(RESOURCE_NAME_PATH),
        RelationalOperator.EQ,
        ConstantExpression.of(resourceName));
  }

  private static RelationalExpression getConfigNonNullRelationalFilter() {
    return RelationalExpression.of(
        IdentifierExpression.of(CONFIG_PATH),
        RelationalOperator.NEQ,
        ConstantExpression.of((String) null));
  }

  private static RelationalExpression getConfigPathExistsRelationalFilter(String configPath) {
    return RelationalExpression.of(
        IdentifierExpression.of(configPath),
        RelationalOperator.EXISTS,
        ConstantExpression.of(true));
  }
}
