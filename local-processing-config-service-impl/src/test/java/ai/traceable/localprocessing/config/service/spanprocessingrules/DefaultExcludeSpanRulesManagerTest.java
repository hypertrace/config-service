package ai.traceable.localprocessing.config.service.spanprocessingrules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import ai.traceable.localprocessing.config.service.spanprocessingrules.excludespanrules.DefaultExcludeSpanRulesManager;
import ai.traceable.localprocessing.config.service.spanprocessingrules.excludespanrules.ExcludeSpanRulesConfig;
import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRule;
import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRuleInfo;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.config.span.processing.utils.SpanFilterMatcher;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.ListValue;
import org.hypertrace.span.processing.config.service.v1.LogicalOperator;
import org.hypertrace.span.processing.config.service.v1.LogicalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.RelationalOperator;
import org.hypertrace.span.processing.config.service.v1.RelationalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.RuleType;
import org.hypertrace.span.processing.config.service.v1.SpanFilter;
import org.hypertrace.span.processing.config.service.v1.SpanFilterValue;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultExcludeSpanRulesManagerTest {

  private static final String IN_OPERATOR_RULE_ID = "id0";
  private static final String AGENT_EXCLUDED_RULE_ID = "id1";
  private static final String AGENT_SUPPORTED_RULE_ID = "id2";

  @Mock private SpanProcessingConfigServiceBlockingStub mockConfigServiceBlockingStub;
  @Mock private SpanFilterMatcher mockSpanFilterMatcher;
  @Mock private ClientConfig mockClientConfig;
  @Mock private RequestContext mockRequestContext;
  private DefaultExcludeSpanRulesManager excludeSpanRulesManager;

  @BeforeEach
  void setup() {
    excludeSpanRulesManager =
        new DefaultExcludeSpanRulesManager(
            mockConfigServiceBlockingStub,
            mockSpanFilterMatcher,
            mockClientConfig,
            new ExcludeSpanRulesConfig(
                ConfigFactory.parseMap(
                    Map.of(
                        "span.exclusion.rules.agent.unsupported.rule.ids",
                        List.of(AGENT_EXCLUDED_RULE_ID)))));
  }

  @Test
  void test_inOperatorNotSupported() {
    ExcludeSpanRule excludeSpanRule =
        ExcludeSpanRule.newBuilder()
            .setId(IN_OPERATOR_RULE_ID)
            .setRuleInfo(
                ExcludeSpanRuleInfo.newBuilder()
                    .setDisabled(false)
                    .setName("name")
                    .setType(RuleType.RULE_TYPE_SYSTEM)
                    .setFilter(
                        SpanFilter.newBuilder()
                            .setRelationalSpanFilter(
                                RelationalSpanFilterExpression.newBuilder()
                                    .setSpanAttributeKey("key")
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_IN)
                                    .setRightOperand(
                                        SpanFilterValue.newBuilder()
                                            .setListValue(
                                                ListValue.newBuilder()
                                                    .addValues(
                                                        SpanFilterValue.newBuilder()
                                                            .setStringValue("value")))))))
            .build();
    List<ExcludeSpanProcessingRule> actual =
        excludeSpanRulesManager.getAllMatchingExcludeSpanProcessingRules(
            mockRequestContext, List.of(excludeSpanRule), "service", Optional.of("environment"));
    assertEquals(actual.size(), 0);
  }

  @Test
  void test_spanRuleExcluded_inListOfSpanFilters() {
    ExcludeSpanRule excludeSpanRule =
        ExcludeSpanRule.newBuilder()
            .setId(IN_OPERATOR_RULE_ID)
            .setRuleInfo(
                ExcludeSpanRuleInfo.newBuilder()
                    .setDisabled(false)
                    .setName("name")
                    .setType(RuleType.RULE_TYPE_SYSTEM)
                    .setFilter(
                        SpanFilter.newBuilder()
                            .setLogicalSpanFilter(
                                LogicalSpanFilterExpression.newBuilder()
                                    .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                                    .addOperands(
                                        SpanFilter.newBuilder()
                                            .setRelationalSpanFilter(
                                                RelationalSpanFilterExpression.newBuilder()
                                                    .setSpanAttributeKey("key")
                                                    .setOperator(
                                                        RelationalOperator
                                                            .RELATIONAL_OPERATOR_EQUALS)
                                                    .setRightOperand(
                                                        SpanFilterValue.newBuilder()
                                                            .setStringValue("value"))))
                                    .addOperands(
                                        SpanFilter.newBuilder()
                                            .setRelationalSpanFilter(
                                                RelationalSpanFilterExpression.newBuilder()
                                                    .setSpanAttributeKey("key")
                                                    .setOperator(
                                                        RelationalOperator.RELATIONAL_OPERATOR_IN)
                                                    .setRightOperand(
                                                        SpanFilterValue.newBuilder()
                                                            .setListValue(
                                                                ListValue.newBuilder()
                                                                    .addValues(
                                                                        SpanFilterValue.newBuilder()
                                                                            .setStringValue(
                                                                                "value")))))))))
            .build();
    List<ExcludeSpanProcessingRule> actual =
        excludeSpanRulesManager.getAllMatchingExcludeSpanProcessingRules(
            mockRequestContext, List.of(excludeSpanRule), "service", Optional.of("environment"));
    assertEquals(actual.size(), 0);
  }

  @Test
  void test_ruleIdExcluded() {
    ExcludeSpanRule excludeSpanRule =
        ExcludeSpanRule.newBuilder()
            .setId(AGENT_EXCLUDED_RULE_ID)
            .setRuleInfo(
                ExcludeSpanRuleInfo.newBuilder()
                    .setDisabled(false)
                    .setName("name")
                    .setType(RuleType.RULE_TYPE_SYSTEM)
                    .setFilter(
                        SpanFilter.newBuilder()
                            .setRelationalSpanFilter(
                                RelationalSpanFilterExpression.newBuilder()
                                    .setSpanAttributeKey("key")
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQUALS)
                                    .setRightOperand(
                                        SpanFilterValue.newBuilder().setStringValue("value")))))
            .build();
    List<ExcludeSpanProcessingRule> actual =
        excludeSpanRulesManager.getAllMatchingExcludeSpanProcessingRules(
            mockRequestContext, List.of(excludeSpanRule), "service", Optional.of("environment"));
    assertEquals(actual.size(), 0);
  }

  @Test
  void test_ruleIdNotExcluded() {
    ExcludeSpanRule excludeSpanRule =
        ExcludeSpanRule.newBuilder()
            .setId(AGENT_SUPPORTED_RULE_ID)
            .setRuleInfo(
                ExcludeSpanRuleInfo.newBuilder()
                    .setDisabled(false)
                    .setName("name")
                    .setType(RuleType.RULE_TYPE_SYSTEM)
                    .setFilter(
                        SpanFilter.newBuilder()
                            .setRelationalSpanFilter(
                                RelationalSpanFilterExpression.newBuilder()
                                    .setSpanAttributeKey("key")
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQUALS)
                                    .setRightOperand(
                                        SpanFilterValue.newBuilder().setStringValue("value")))))
            .build();
    when(mockSpanFilterMatcher.matchesEnvironment(
            eq(excludeSpanRule.getRuleInfo().getFilter()), eq(Optional.of("environment"))))
        .thenReturn(true);
    when(mockSpanFilterMatcher.matchesServiceName(
            eq(excludeSpanRule.getRuleInfo().getFilter()), eq("service")))
        .thenReturn(true);
    List<ExcludeSpanProcessingRule> expected = List.of(buildExpectedExcludeSpanProcessingRule());
    List<ExcludeSpanProcessingRule> actual =
        excludeSpanRulesManager.getAllMatchingExcludeSpanProcessingRules(
            mockRequestContext, List.of(excludeSpanRule), "service", Optional.of("environment"));
    assertEquals(expected, actual);
  }

  @Test
  void test_spanRuleNotExcluded_inListOfSpanFilters() {
    ExcludeSpanRule excludeSpanRule =
        ExcludeSpanRule.newBuilder()
            .setId(AGENT_SUPPORTED_RULE_ID)
            .setRuleInfo(
                ExcludeSpanRuleInfo.newBuilder()
                    .setDisabled(false)
                    .setName("name")
                    .setType(RuleType.RULE_TYPE_SYSTEM)
                    .setFilter(
                        SpanFilter.newBuilder()
                            .setLogicalSpanFilter(
                                LogicalSpanFilterExpression.newBuilder()
                                    .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                                    .addOperands(
                                        SpanFilter.newBuilder()
                                            .setRelationalSpanFilter(
                                                RelationalSpanFilterExpression.newBuilder()
                                                    .setSpanAttributeKey("key")
                                                    .setOperator(
                                                        RelationalOperator
                                                            .RELATIONAL_OPERATOR_EQUALS)
                                                    .setRightOperand(
                                                        SpanFilterValue.newBuilder()
                                                            .setStringValue("value"))))
                                    .addOperands(
                                        SpanFilter.newBuilder()
                                            .setRelationalSpanFilter(
                                                RelationalSpanFilterExpression.newBuilder()
                                                    .setSpanAttributeKey("key1")
                                                    .setOperator(
                                                        RelationalOperator
                                                            .RELATIONAL_OPERATOR_EQUALS)
                                                    .setRightOperand(
                                                        SpanFilterValue.newBuilder()
                                                            .setStringValue("value1")))))))
            .build();
    when(mockSpanFilterMatcher.matchesEnvironment(
            eq(excludeSpanRule.getRuleInfo().getFilter()), eq(Optional.of("environment"))))
        .thenReturn(true);
    when(mockSpanFilterMatcher.matchesServiceName(
            eq(excludeSpanRule.getRuleInfo().getFilter()), eq("service")))
        .thenReturn(true);
    List<ExcludeSpanProcessingRule> expected =
        List.of(buildExpectedExcludeSpanProcessingRuleForListOfFilters());
    List<ExcludeSpanProcessingRule> actual =
        excludeSpanRulesManager.getAllMatchingExcludeSpanProcessingRules(
            mockRequestContext, List.of(excludeSpanRule), "service", Optional.of("environment"));
    assertEquals(expected, actual);
  }

  private ExcludeSpanProcessingRule buildExpectedExcludeSpanProcessingRule() {
    return ExcludeSpanProcessingRule.newBuilder()
        .setExcludeSpanProcessingRuleInfo(
            ExcludeSpanProcessingRuleInfo.newBuilder()
                .setId(AGENT_SUPPORTED_RULE_ID)
                .setFilter(
                    ai.traceable.localprocessing.config.service.v1.SpanFilter.newBuilder()
                        .setRelationalFilter(
                            ai.traceable.localprocessing.config.service.v1
                                .RelationalSpanFilterExpression.newBuilder()
                                .setSpanAttributeKey("key")
                                .setOperator(
                                    ai.traceable.localprocessing.config.service.v1
                                        .RelationalOperator.RELATIONAL_OPERATOR_EQUALS)
                                .setRightOperand(
                                    ai.traceable.localprocessing.config.service.v1.SpanFilterValue
                                        .newBuilder()
                                        .setStringValue("value")))
                        .build()))
        .build();
  }

  private ExcludeSpanProcessingRule buildExpectedExcludeSpanProcessingRuleForListOfFilters() {
    return ExcludeSpanProcessingRule.newBuilder()
        .setExcludeSpanProcessingRuleInfo(
            ExcludeSpanProcessingRuleInfo.newBuilder()
                .setId(AGENT_SUPPORTED_RULE_ID)
                .setFilter(
                    ai.traceable.localprocessing.config.service.v1.SpanFilter.newBuilder()
                        .setLogicalFilter(
                            ai.traceable.localprocessing.config.service.v1
                                .LogicalSpanFilterExpression.newBuilder()
                                .setOperator(
                                    ai.traceable.localprocessing.config.service.v1.LogicalOperator
                                        .LOGICAL_OPERATOR_AND)
                                .addOperands(
                                    ai.traceable.localprocessing.config.service.v1.SpanFilter
                                        .newBuilder()
                                        .setRelationalFilter(
                                            ai.traceable.localprocessing.config.service.v1
                                                .RelationalSpanFilterExpression.newBuilder()
                                                .setSpanAttributeKey("key")
                                                .setOperator(
                                                    ai.traceable.localprocessing.config.service.v1
                                                        .RelationalOperator
                                                        .RELATIONAL_OPERATOR_EQUALS)
                                                .setRightOperand(
                                                    ai.traceable.localprocessing.config.service.v1
                                                        .SpanFilterValue.newBuilder()
                                                        .setStringValue("value"))))
                                .addOperands(
                                    ai.traceable.localprocessing.config.service.v1.SpanFilter
                                        .newBuilder()
                                        .setRelationalFilter(
                                            ai.traceable.localprocessing.config.service.v1
                                                .RelationalSpanFilterExpression.newBuilder()
                                                .setSpanAttributeKey("key1")
                                                .setOperator(
                                                    ai.traceable.localprocessing.config.service.v1
                                                        .RelationalOperator
                                                        .RELATIONAL_OPERATOR_EQUALS)
                                                .setRightOperand(
                                                    ai.traceable.localprocessing.config.service.v1
                                                        .SpanFilterValue.newBuilder()
                                                        .setStringValue("value1"))))
                                .build())))
        .build();
  }
}
