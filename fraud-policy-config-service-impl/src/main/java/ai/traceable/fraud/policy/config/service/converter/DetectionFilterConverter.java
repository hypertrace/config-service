package ai.traceable.fraud.policy.config.service.converter;

import static ai.traceable.fraud.policy.config.service.converter.JexlExpressionUtils.escapeJexlString;

import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.GenericMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.fraud.datamodel.event.kind.v1.OperatorType;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyDetectionFilter;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyLogicalOperator;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyRelationalFilter;
import com.google.protobuf.Value;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Converts AbusePolicyDetectionFilter trees into EDS MatchCondition protos.
 *
 * <p>Relational filters are converted to GenericMatchCondition with JEXL boolean expressions.
 * Logical filters (AND/OR) are converted to LogicalMatchCondition composites.
 */
@Slf4j
@Singleton
public class DetectionFilterConverter {

  /**
   * Converts a list of detection filters to a single MatchCondition. Multiple top-level filters are
   * ANDed together.
   */
  public Optional<MatchCondition> convert(
      List<AbusePolicyDetectionFilter> filters,
      Map<String, List<DerivationRule>> entityRulesMap,
      Map<String, String> entityVariableNames) {
    if (filters.isEmpty()) {
      return Optional.empty();
    }
    if (filters.size() == 1) {
      return convertFilter(filters.get(0), entityRulesMap, entityVariableNames);
    }
    LogicalMatchCondition.Builder logicalBuilder =
        LogicalMatchCondition.newBuilder()
            .setOperator(LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND);
    filters.stream()
        .map(filter -> convertFilter(filter, entityRulesMap, entityVariableNames))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(logicalBuilder::addConditions);
    if (logicalBuilder.getConditionsList().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        MatchCondition.newBuilder().setLogicalMatchCondition(logicalBuilder.build()).build());
  }

  private Optional<MatchCondition> convertFilter(
      AbusePolicyDetectionFilter filter,
      Map<String, List<DerivationRule>> entityRulesMap,
      Map<String, String> entityVariableNames) {
    if (filter.hasRelationalFilter()) {
      return convertRelationalFilter(
          filter.getRelationalFilter(), entityRulesMap, entityVariableNames);
    }
    if (filter.hasLogicalFilter()) {
      LogicalMatchOperator operator =
          filter.getLogicalFilter().getOperator()
                  == AbusePolicyLogicalOperator.ABUSE_POLICY_LOGICAL_OPERATOR_OR
              ? LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR
              : LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND;
      LogicalMatchCondition.Builder logicalBuilder =
          LogicalMatchCondition.newBuilder().setOperator(operator);
      filter.getLogicalFilter().getOperandsList().stream()
          .map(operand -> convertFilter(operand, entityRulesMap, entityVariableNames))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .forEach(logicalBuilder::addConditions);
      if (logicalBuilder.getConditionsList().isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(
          MatchCondition.newBuilder().setLogicalMatchCondition(logicalBuilder.build()).build());
    }
    return Optional.empty();
  }

  private Optional<MatchCondition> convertRelationalFilter(
      AbusePolicyRelationalFilter filter,
      Map<String, List<DerivationRule>> entityRulesMap,
      Map<String, String> entityVariableNames) {
    String entityId = filter.getDerivedEntityId();
    // Entity is a variable in rule_variables; use variable name as LHS reference
    if (!entityRulesMap.containsKey(entityId) || entityRulesMap.get(entityId).isEmpty()) {
      log.warn("No derivation rules for filter derived entity: {}", entityId);
      return Optional.empty();
    }
    String lhsJexl = entityVariableNames.getOrDefault(entityId, entityId);

    OperatorType opType = filter.getOperator();

    String rhsValue = "";
    if (filter.hasLiteralValues() && !filter.getLiteralValues().getValuesList().isEmpty()) {
      List<Value> values = filter.getLiteralValues().getValuesList();
      if (values.size() == 1) {
        rhsValue = JexlExpressionUtils.valueToString(values.get(0));
      } else {
        // Multiple values: build list-contains check
        String valueList =
            values.stream()
                .map(v -> escapeJexlString(JexlExpressionUtils.valueToString(v)))
                .collect(Collectors.joining("', '", "['", "']"));
        String containsExpr = valueList + ".contains(" + lhsJexl + ")";
        if (isNegatedOperator(opType)) {
          containsExpr = "!" + containsExpr;
        }
        return buildJexlMatchCondition(containsExpr);
      }
    }

    String jexlExpr = buildComparisonJexl(lhsJexl, opType, rhsValue);
    if (jexlExpr == null) {
      log.warn("Unsupported operator type for filter JEXL conversion: {}", opType);
      return Optional.empty();
    }
    return buildJexlMatchCondition(jexlExpr);
  }

  private String buildComparisonJexl(String lhsJexl, OperatorType opType, String rhsValue) {
    String escaped = escapeJexlString(rhsValue);
    switch (opType) {
      case OPERATOR_TYPE_STRING_EQUALS:
        return lhsJexl + ".equals('" + escaped + "')";
      case OPERATOR_TYPE_STRING_NOT_EQUALS:
        return "!" + lhsJexl + ".equals('" + escaped + "')";
      case OPERATOR_TYPE_NUMERIC_EQUALS:
      case OPERATOR_TYPE_BOOLEAN_EQUALS:
        return lhsJexl + " == " + rhsValue;
      case OPERATOR_TYPE_NUMERIC_NOT_EQUALS:
        return lhsJexl + " != " + rhsValue;
      case OPERATOR_TYPE_LESS_THAN:
        return lhsJexl + " < " + rhsValue;
      case OPERATOR_TYPE_LESS_THAN_OR_EQUALS:
        return lhsJexl + " <= " + rhsValue;
      case OPERATOR_TYPE_GREATER_THAN:
        return lhsJexl + " > " + rhsValue;
      case OPERATOR_TYPE_GREATER_THAN_OR_EQUALS:
        return lhsJexl + " >= " + rhsValue;
      case OPERATOR_TYPE_CONTAINS:
        return lhsJexl + ".contains('" + escaped + "')";
      case OPERATOR_TYPE_NOT_CONTAINS:
        return "!" + lhsJexl + ".contains('" + escaped + "')";
      case OPERATOR_TYPE_STARTS_WITH:
        return lhsJexl + ".startsWith('" + escaped + "')";
      case OPERATOR_TYPE_ENDS_WITH:
        return lhsJexl + ".endsWith('" + escaped + "')";
      case OPERATOR_TYPE_MATCHES_REGEX:
        return lhsJexl + " =~ '" + escaped + "'";
      default:
        return null;
    }
  }

  private boolean isNegatedOperator(OperatorType opType) {
    return opType == OperatorType.OPERATOR_TYPE_STRING_NOT_EQUALS
        || opType == OperatorType.OPERATOR_TYPE_NUMERIC_NOT_EQUALS
        || opType == OperatorType.OPERATOR_TYPE_NOT_CONTAINS;
  }

  private Optional<MatchCondition> buildJexlMatchCondition(String jexlExpr) {
    return Optional.of(
        MatchCondition.newBuilder()
            .setGenericMatchCondition(
                GenericMatchCondition.newBuilder()
                    .setJexlExpression(
                        JexlExpressionConfig.newBuilder().setJexlExpression(jexlExpr)))
            .build());
  }
}
