package ai.traceable.localprocessing.config.service.spanprocessingrules;

import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRule;
import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRuleInfo;
import ai.traceable.localprocessing.config.service.v1.RateLimit;
import ai.traceable.localprocessing.config.service.v1.RateLimitConfig;
import ai.traceable.localprocessing.config.service.v1.WindowedRateLimit;
import com.google.protobuf.Duration;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.Field;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesResponse;
import org.hypertrace.span.processing.config.service.v1.LogicalOperator;
import org.hypertrace.span.processing.config.service.v1.LogicalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.RelationalOperator;
import org.hypertrace.span.processing.config.service.v1.RelationalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.SpanFilter;
import org.hypertrace.span.processing.config.service.v1.SpanFilterValue;

public class SpanProcessingRulesManagerTestUtils {
  public static ExcludeSpanProcessingRule buildExpectedExcludeSpanProcessingRule() {
    return ExcludeSpanProcessingRule.newBuilder()
        .setExcludeSpanProcessingRuleInfo(
            ExcludeSpanProcessingRuleInfo.newBuilder()
                .setId("id")
                .setFilter(
                    buildLogicalFilterLocalProcessing(
                        ai.traceable.localprocessing.config.service.v1.LogicalOperator
                            .LOGICAL_OPERATOR_AND,
                        List.of(
                            buildLogicalFilterLocalProcessing(
                                ai.traceable.localprocessing.config.service.v1.LogicalOperator
                                    .LOGICAL_OPERATOR_OR,
                                List.of(
                                    buildRelationalFilter(
                                        "key",
                                        ai.traceable.localprocessing.config.service.v1
                                            .RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                                        "val"),
                                    buildRelationalFilter(
                                        "key",
                                        ai.traceable.localprocessing.config.service.v1
                                            .RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                                        "val"))),
                            buildRelationalFilter(
                                "url",
                                ai.traceable.localprocessing.config.service.v1.RelationalOperator
                                    .RELATIONAL_OPERATOR_EQUALS,
                                "url"))))
                .build())
        .build();
  }

  public static ExcludeSpanProcessingRule
      buildExpectedExcludeSpanProcessingRuleServiceNamesAndEnvironmentsProcessed() {
    return ExcludeSpanProcessingRule.newBuilder()
        .setExcludeSpanProcessingRuleInfo(
            ExcludeSpanProcessingRuleInfo.newBuilder()
                .setId("id")
                .setFilter(
                    buildLogicalFilterLocalProcessing(
                        ai.traceable.localprocessing.config.service.v1.LogicalOperator
                            .LOGICAL_OPERATOR_OR,
                        List.of(
                            buildRelationalFilter(
                                "key",
                                ai.traceable.localprocessing.config.service.v1.RelationalOperator
                                    .RELATIONAL_OPERATOR_CONTAINS,
                                "val"),
                            buildRelationalFilter(
                                "key",
                                ai.traceable.localprocessing.config.service.v1.RelationalOperator
                                    .RELATIONAL_OPERATOR_CONTAINS,
                                "val"))))
                .build())
        .build();
  }

  public static GetAllExcludeSpanRulesResponse buildGetAllExcludeSpanRulesResponse(
      boolean disabled) {
    return GetAllExcludeSpanRulesResponse.newBuilder()
        .addRuleDetails(
            ExcludeSpanRuleDetails.newBuilder()
                .setRule(
                    ExcludeSpanRule.newBuilder()
                        .setId("id")
                        .setRuleInfo(
                            ExcludeSpanRuleInfo.newBuilder()
                                .setName("name")
                                .setDisabled(disabled)
                                .setFilter(
                                    buildLogicalFilterSpanProcessing(
                                        LogicalOperator.LOGICAL_OPERATOR_AND,
                                        List.of(
                                            buildLogicalFilterSpanProcessing(
                                                LogicalOperator.LOGICAL_OPERATOR_OR,
                                                List.of(
                                                    buildRelationalFilter(
                                                        null,
                                                        "key",
                                                        RelationalOperator
                                                            .RELATIONAL_OPERATOR_CONTAINS,
                                                        "val"),
                                                    buildRelationalFilter(
                                                        null,
                                                        "key",
                                                        RelationalOperator
                                                            .RELATIONAL_OPERATOR_CONTAINS,
                                                        "val"))),
                                            buildRelationalFilter(
                                                Field.FIELD_URL,
                                                null,
                                                RelationalOperator.RELATIONAL_OPERATOR_EQUALS,
                                                "url"))))
                                .build())
                        .build()))
        .build();
  }

  public static GetAllExcludeSpanRulesResponse
      buildGetAllExcludeSpanRulesResponseEnvironmentFilter() {
    return GetAllExcludeSpanRulesResponse.newBuilder()
        .addRuleDetails(
            ExcludeSpanRuleDetails.newBuilder()
                .setRule(
                    ExcludeSpanRule.newBuilder()
                        .setId("id")
                        .setRuleInfo(
                            ExcludeSpanRuleInfo.newBuilder()
                                .setName("name")
                                .setFilter(
                                    buildLogicalFilterSpanProcessing(
                                        LogicalOperator.LOGICAL_OPERATOR_AND,
                                        List.of(
                                            buildLogicalFilterSpanProcessing(
                                                LogicalOperator.LOGICAL_OPERATOR_OR,
                                                List.of(
                                                    buildRelationalFilter(
                                                        null,
                                                        "key",
                                                        RelationalOperator
                                                            .RELATIONAL_OPERATOR_CONTAINS,
                                                        "val"),
                                                    buildRelationalFilter(
                                                        null,
                                                        "key",
                                                        RelationalOperator
                                                            .RELATIONAL_OPERATOR_CONTAINS,
                                                        "val"))),
                                            buildRelationalFilter(
                                                Field.FIELD_ENVIRONMENT_NAME,
                                                null,
                                                RelationalOperator.RELATIONAL_OPERATOR_IN,
                                                List.of("value1", "value")))))
                                .build())
                        .build()))
        .build();
  }

  public static GetAllExcludeSpanRulesResponse
      buildGetAllExcludeSpanRulesResponseServiceNameFilter() {
    return GetAllExcludeSpanRulesResponse.newBuilder()
        .addRuleDetails(
            ExcludeSpanRuleDetails.newBuilder()
                .setRule(
                    ExcludeSpanRule.newBuilder()
                        .setId("id")
                        .setRuleInfo(
                            ExcludeSpanRuleInfo.newBuilder()
                                .setName("name")
                                .setFilter(
                                    buildLogicalFilterSpanProcessing(
                                        LogicalOperator.LOGICAL_OPERATOR_AND,
                                        List.of(
                                            buildLogicalFilterSpanProcessing(
                                                LogicalOperator.LOGICAL_OPERATOR_OR,
                                                List.of(
                                                    buildRelationalFilter(
                                                        null,
                                                        "key",
                                                        RelationalOperator
                                                            .RELATIONAL_OPERATOR_CONTAINS,
                                                        "val"),
                                                    buildRelationalFilter(
                                                        null,
                                                        "key",
                                                        RelationalOperator
                                                            .RELATIONAL_OPERATOR_CONTAINS,
                                                        "val"))),
                                            buildRelationalFilter(
                                                Field.FIELD_SERVICE_NAME,
                                                null,
                                                RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                                                "val"))))
                                .build())
                        .build()))
        .build();
  }

  private static SpanFilter buildLogicalFilterSpanProcessing(
      org.hypertrace.span.processing.config.service.v1.LogicalOperator operator,
      List<org.hypertrace.span.processing.config.service.v1.SpanFilter> filters) {
    return SpanFilter.newBuilder()
        .setLogicalSpanFilter(
            LogicalSpanFilterExpression.newBuilder()
                .setOperator(operator)
                .addAllOperands(filters)
                .build())
        .build();
  }

  private static ai.traceable.localprocessing.config.service.v1.SpanFilter
      buildLogicalFilterLocalProcessing(
          ai.traceable.localprocessing.config.service.v1.LogicalOperator operator,
          List<ai.traceable.localprocessing.config.service.v1.SpanFilter> filters) {
    return ai.traceable.localprocessing.config.service.v1.SpanFilter.newBuilder()
        .setLogicalFilter(
            ai.traceable.localprocessing.config.service.v1.LogicalSpanFilterExpression.newBuilder()
                .setOperator(operator)
                .addAllOperands(filters)
                .build())
        .build();
  }

  private static SpanFilter buildRelationalFilter(
      Field field, String spanAttributeKey, RelationalOperator operator, String rhs) {
    RelationalSpanFilterExpression.Builder relationalSpanFilterExpressionBuilder =
        RelationalSpanFilterExpression.newBuilder();
    if (spanAttributeKey == null) {
      relationalSpanFilterExpressionBuilder.setField(field);
    } else {
      relationalSpanFilterExpressionBuilder.setSpanAttributeKey(spanAttributeKey);
    }
    return SpanFilter.newBuilder()
        .setRelationalSpanFilter(
            relationalSpanFilterExpressionBuilder
                .setOperator(operator)
                .setRightOperand(SpanFilterValue.newBuilder().setStringValue(rhs).build())
                .build())
        .build();
  }

  private static SpanFilter buildRelationalFilter(
      Field field, String spanAttributeKey, RelationalOperator operator, List<String> rhs) {
    RelationalSpanFilterExpression.Builder relationalSpanFilterExpressionBuilder =
        RelationalSpanFilterExpression.newBuilder();
    if (spanAttributeKey == null) {
      relationalSpanFilterExpressionBuilder.setField(field);
    } else {
      relationalSpanFilterExpressionBuilder.setSpanAttributeKey(spanAttributeKey);
    }
    return SpanFilter.newBuilder()
        .setRelationalSpanFilter(
            relationalSpanFilterExpressionBuilder
                .setOperator(operator)
                .setRightOperand(
                    SpanFilterValue.newBuilder()
                        .setListValue(
                            org.hypertrace.span.processing.config.service.v1.ListValue.newBuilder()
                                .addAllValues(
                                    rhs.stream()
                                        .map(
                                            val ->
                                                SpanFilterValue.newBuilder()
                                                    .setStringValue(val)
                                                    .build())
                                        .collect(Collectors.toUnmodifiableList()))
                                .build())
                        .build())
                .build())
        .build();
  }

  private static ai.traceable.localprocessing.config.service.v1.SpanFilter buildRelationalFilter(
      String spanAttributeKey,
      ai.traceable.localprocessing.config.service.v1.RelationalOperator operator,
      String rhs) {
    return ai.traceable.localprocessing.config.service.v1.SpanFilter.newBuilder()
        .setRelationalFilter(
            ai.traceable.localprocessing.config.service.v1.RelationalSpanFilterExpression
                .newBuilder()
                .setOperator(operator)
                .setSpanAttributeKey(spanAttributeKey)
                .setRightOperand(
                    ai.traceable.localprocessing.config.service.v1.SpanFilterValue.newBuilder()
                        .setStringValue(rhs)
                        .build())
                .build())
        .build();
  }

  public static RateLimitConfig buildExpectedTenantSpecificRateLimitConfig() {
    return RateLimitConfig.newBuilder()
        .setApiEndpointCacheDuration(Duration.newBuilder().setSeconds(200).build())
        .setTraceLimitPerEndpoint(
            RateLimit.newBuilder()
                .setFixedWindowLimit(
                    WindowedRateLimit.newBuilder()
                        .setQuantityAllowed(100)
                        .setWindowDuration(Duration.newBuilder().setSeconds(5).build())
                        .build())
                .build())
        .setTraceLimitGlobal(
            RateLimit.newBuilder()
                .setFixedWindowLimit(
                    WindowedRateLimit.newBuilder()
                        .setQuantityAllowed(100000)
                        .setWindowDuration(Duration.newBuilder().setSeconds(10).build())
                        .build())
                .build())
        .build();
  }

  public static Config buildConfig() {
    return ConfigFactory.parseMap(
        Map.of(
            "sampling.config",
            Map.of(
                "rate.limit.config",
                Map.of(
                    "tenant",
                    Map.of(
                        "apiEndpointCacheDuration",
                        "200s",
                        "traceLimitPerEndpoint",
                        Map.of(
                            "fixedWindowLimit",
                            Map.of("quantityAllowed", 100, "windowDuration", "5s")),
                        "traceLimitGlobal",
                        Map.of(
                            "fixedWindowLimit",
                            Map.of("quantityAllowed", 100000, "windowDuration", "10s")))))));
  }
}
