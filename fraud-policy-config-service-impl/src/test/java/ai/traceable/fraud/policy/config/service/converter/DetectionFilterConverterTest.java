package ai.traceable.fraud.policy.config.service.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.fraud.datamodel.event.kind.v1.OperatorType;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyDetectionFilter;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyLiteralValues;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyLogicalFilter;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyLogicalOperator;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyRelationalFilter;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DetectionFilterConverterTest {

  private DetectionFilterConverter converter;
  private Map<String, List<DerivationRule>> entityRulesMap;

  @BeforeEach
  void setUp() {
    converter = new DetectionFilterConverter();
    // Entities are now variables; LHS references the variable name (entity ID)
    entityRulesMap =
        Map.of(
            "entity_ip", List.of(dummyRule("$s.ip_address")),
            "entity_header", List.of(dummyRule("$s.request_headers.get('x-custom')")),
            "entity_ua", List.of(dummyRule("$s.user_agent")),
            "entity_status", List.of(dummyRule("$s.status_code")));
  }

  private static DerivationRule dummyRule(String jexl) {
    return DerivationRule.newBuilder()
        .setTransformationConfig(
            DataTransformationConfig.newBuilder()
                .setJexlExpression(JexlExpressionConfig.newBuilder().setJexlExpression(jexl)))
        .build();
  }

  @Test
  void convert_emptyFilters_returnsEmpty() {
    Optional<MatchCondition> result = converter.convert(List.of(), entityRulesMap, Map.of());
    assertFalse(result.isPresent());
  }

  @Test
  void convert_singleStringEquals() {
    AbusePolicyDetectionFilter filter =
        relationalFilter("entity_ip", OperatorType.OPERATOR_TYPE_STRING_EQUALS, "1.2.3.4");

    Optional<MatchCondition> result = converter.convert(List.of(filter), entityRulesMap, Map.of());

    assertTrue(result.isPresent());
    assertEquals(
        "entity_ip.equals('1.2.3.4')",
        result.get().getGenericMatchCondition().getJexlExpression().getJexlExpression());
  }

  @Test
  void convert_stringNotEquals() {
    AbusePolicyDetectionFilter filter =
        relationalFilter("entity_ip", OperatorType.OPERATOR_TYPE_STRING_NOT_EQUALS, "1.2.3.4");

    Optional<MatchCondition> result = converter.convert(List.of(filter), entityRulesMap, Map.of());

    assertTrue(result.isPresent());
    assertEquals(
        "!entity_ip.equals('1.2.3.4')",
        result.get().getGenericMatchCondition().getJexlExpression().getJexlExpression());
  }

  @Test
  void convert_contains() {
    AbusePolicyDetectionFilter filter =
        relationalFilter("entity_ua", OperatorType.OPERATOR_TYPE_CONTAINS, "bot");

    Optional<MatchCondition> result = converter.convert(List.of(filter), entityRulesMap, Map.of());

    assertTrue(result.isPresent());
    assertEquals(
        "entity_ua.contains('bot')",
        result.get().getGenericMatchCondition().getJexlExpression().getJexlExpression());
  }

  @Test
  void convert_notContains() {
    AbusePolicyDetectionFilter filter =
        relationalFilter("entity_ua", OperatorType.OPERATOR_TYPE_NOT_CONTAINS, "bot");

    Optional<MatchCondition> result = converter.convert(List.of(filter), entityRulesMap, Map.of());

    assertTrue(result.isPresent());
    assertEquals(
        "!entity_ua.contains('bot')",
        result.get().getGenericMatchCondition().getJexlExpression().getJexlExpression());
  }

  @Test
  void convert_startsWith() {
    AbusePolicyDetectionFilter filter =
        relationalFilter("entity_header", OperatorType.OPERATOR_TYPE_STARTS_WITH, "Bearer");

    Optional<MatchCondition> result = converter.convert(List.of(filter), entityRulesMap, Map.of());

    assertTrue(result.isPresent());
    assertEquals(
        "entity_header.startsWith('Bearer')",
        result.get().getGenericMatchCondition().getJexlExpression().getJexlExpression());
  }

  @Test
  void convert_endsWith() {
    AbusePolicyDetectionFilter filter =
        relationalFilter("entity_header", OperatorType.OPERATOR_TYPE_ENDS_WITH, ".json");

    Optional<MatchCondition> result = converter.convert(List.of(filter), entityRulesMap, Map.of());

    assertTrue(result.isPresent());
    assertEquals(
        "entity_header.endsWith('.json')",
        result.get().getGenericMatchCondition().getJexlExpression().getJexlExpression());
  }

  @Test
  void convert_matchesRegex() {
    AbusePolicyDetectionFilter filter =
        relationalFilter("entity_ua", OperatorType.OPERATOR_TYPE_MATCHES_REGEX, ".*bot.*");

    Optional<MatchCondition> result = converter.convert(List.of(filter), entityRulesMap, Map.of());

    assertTrue(result.isPresent());
    assertEquals(
        "entity_ua =~ '.*bot.*'",
        result.get().getGenericMatchCondition().getJexlExpression().getJexlExpression());
  }

  @Test
  void convert_numericGreaterThan() {
    AbusePolicyDetectionFilter filter =
        AbusePolicyDetectionFilter.newBuilder()
            .setRelationalFilter(
                AbusePolicyRelationalFilter.newBuilder()
                    .setDerivedEntityId("entity_status")
                    .setOperator(OperatorType.OPERATOR_TYPE_GREATER_THAN)
                    .setLiteralValues(
                        AbusePolicyLiteralValues.newBuilder()
                            .addValues(Value.newBuilder().setNumberValue(400))))
            .build();

    Optional<MatchCondition> result = converter.convert(List.of(filter), entityRulesMap, Map.of());

    assertTrue(result.isPresent());
    assertEquals(
        "entity_status > 400",
        result.get().getGenericMatchCondition().getJexlExpression().getJexlExpression());
  }

  @Test
  void convert_numericLessThanOrEquals() {
    AbusePolicyDetectionFilter filter =
        AbusePolicyDetectionFilter.newBuilder()
            .setRelationalFilter(
                AbusePolicyRelationalFilter.newBuilder()
                    .setDerivedEntityId("entity_status")
                    .setOperator(OperatorType.OPERATOR_TYPE_LESS_THAN_OR_EQUALS)
                    .setLiteralValues(
                        AbusePolicyLiteralValues.newBuilder()
                            .addValues(Value.newBuilder().setNumberValue(200))))
            .build();

    Optional<MatchCondition> result = converter.convert(List.of(filter), entityRulesMap, Map.of());

    assertTrue(result.isPresent());
    assertEquals(
        "entity_status <= 200",
        result.get().getGenericMatchCondition().getJexlExpression().getJexlExpression());
  }

  @Test
  void convert_multipleValues_buildsContainsList() {
    AbusePolicyDetectionFilter filter =
        AbusePolicyDetectionFilter.newBuilder()
            .setRelationalFilter(
                AbusePolicyRelationalFilter.newBuilder()
                    .setDerivedEntityId("entity_ip")
                    .setOperator(OperatorType.OPERATOR_TYPE_STRING_EQUALS)
                    .setLiteralValues(
                        AbusePolicyLiteralValues.newBuilder()
                            .addValues(Value.newBuilder().setStringValue("1.1.1.1"))
                            .addValues(Value.newBuilder().setStringValue("2.2.2.2"))))
            .build();

    Optional<MatchCondition> result = converter.convert(List.of(filter), entityRulesMap, Map.of());

    assertTrue(result.isPresent());
    assertEquals(
        "['1.1.1.1', '2.2.2.2'].contains(entity_ip)",
        result.get().getGenericMatchCondition().getJexlExpression().getJexlExpression());
  }

  @Test
  void convert_logicalOrFilter() {
    AbusePolicyDetectionFilter orFilter =
        AbusePolicyDetectionFilter.newBuilder()
            .setLogicalFilter(
                AbusePolicyLogicalFilter.newBuilder()
                    .setOperator(AbusePolicyLogicalOperator.ABUSE_POLICY_LOGICAL_OPERATOR_OR)
                    .addOperands(
                        relationalFilter(
                            "entity_ip", OperatorType.OPERATOR_TYPE_STRING_EQUALS, "1.2.3.4"))
                    .addOperands(
                        relationalFilter("entity_ua", OperatorType.OPERATOR_TYPE_CONTAINS, "bot")))
            .build();

    Optional<MatchCondition> result =
        converter.convert(List.of(orFilter), entityRulesMap, Map.of());

    assertTrue(result.isPresent());
    assertTrue(result.get().hasLogicalMatchCondition());
    assertEquals(
        LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR,
        result.get().getLogicalMatchCondition().getOperator());
    assertEquals(2, result.get().getLogicalMatchCondition().getConditionsCount());
    assertEquals(
        "entity_ip.equals('1.2.3.4')",
        result
            .get()
            .getLogicalMatchCondition()
            .getConditions(0)
            .getGenericMatchCondition()
            .getJexlExpression()
            .getJexlExpression());
    assertEquals(
        "entity_ua.contains('bot')",
        result
            .get()
            .getLogicalMatchCondition()
            .getConditions(1)
            .getGenericMatchCondition()
            .getJexlExpression()
            .getJexlExpression());
  }

  @Test
  void convert_multipleTopLevelFilters_andedTogether() {
    AbusePolicyDetectionFilter f1 =
        relationalFilter("entity_ip", OperatorType.OPERATOR_TYPE_STRING_EQUALS, "1.2.3.4");
    AbusePolicyDetectionFilter f2 =
        relationalFilter("entity_ua", OperatorType.OPERATOR_TYPE_CONTAINS, "bot");

    Optional<MatchCondition> result = converter.convert(List.of(f1, f2), entityRulesMap, Map.of());

    assertTrue(result.isPresent());
    assertTrue(result.get().hasLogicalMatchCondition());
    assertEquals(
        LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND,
        result.get().getLogicalMatchCondition().getOperator());
    assertEquals(2, result.get().getLogicalMatchCondition().getConditionsCount());
  }

  @Test
  void convert_unresolvedEntity_skipsFilter() {
    AbusePolicyDetectionFilter filter =
        relationalFilter("unknown_entity", OperatorType.OPERATOR_TYPE_STRING_EQUALS, "val");

    Optional<MatchCondition> result = converter.convert(List.of(filter), entityRulesMap, Map.of());

    assertFalse(result.isPresent());
  }

  private static AbusePolicyDetectionFilter relationalFilter(
      String entityId, OperatorType op, String value) {
    return AbusePolicyDetectionFilter.newBuilder()
        .setRelationalFilter(
            AbusePolicyRelationalFilter.newBuilder()
                .setDerivedEntityId(entityId)
                .setOperator(op)
                .setLiteralValues(
                    AbusePolicyLiteralValues.newBuilder()
                        .addValues(Value.newBuilder().setStringValue(value))))
        .build();
  }
}
