package ai.traceable.modsecurity.rule.conversion.clause;

import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.PIPE;

import ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression;
import ai.traceable.modsecurity.rule.secrule.operator.ModsecOperator;
import ai.traceable.modsecurity.rule.secrule.operator.ModsecOperatorExpression;
import ai.traceable.modsecurity.rule.secrule.variables.ModsecVariableMetadataKey;
import java.util.Optional;

public class ModsecOperatorConverter {

  static final ModsecOperatorExpression COUNT_EQUALS_ZERO_OPERATOR_EXPRESSION =
      new ModsecOperatorExpression(ModsecOperator.EQUALS, false, "0");

  ModsecOperatorExpression getValueOperatorExpression(CustomModsecMatchExpression expression) {
    ModsecOperator operator;
    boolean negate = false;

    switch (expression.getValueMatchOperator()) {
      case MATCH_OPERATOR_NOT_EQUAL:
        negate = true;
      case MATCH_OPERATOR_EQUALS:
        operator = ModsecOperator.STRING_EQUALS;
        break;
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        negate = true;
      case MATCH_OPERATOR_MATCHES_REGEX:
        operator = ModsecOperator.MATCHES_REGEX;
        break;
      case MATCH_OPERATOR_NOT_CONTAIN:
        negate = true;
      case MATCH_OPERATOR_CONTAINS:
        operator = ModsecOperator.CONTAINS;
        break;
      case MATCH_OPERATOR_GREATER_THAN:
        operator = ModsecOperator.GREATER_THAN;
        break;
      case MATCH_OPERATOR_LESS_THAN:
        operator = ModsecOperator.LESS_THAN;
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unknown valueMatchOperator: %s", expression.getValueMatchOperator()));
    }

    return new ModsecOperatorExpression(operator, negate, expression.getMatchValue());
  }

  Optional<ModsecOperatorExpression> getOnlyPositiveValueOperatorExpression(
      CustomModsecMatchExpression expression) {
    Optional<ModsecOperator> operator;
    switch (expression.getValueMatchOperator()) {
      case MATCH_OPERATOR_EQUALS:
        operator = Optional.of(ModsecOperator.STRING_EQUALS);
        break;
      case MATCH_OPERATOR_MATCHES_REGEX:
        operator = Optional.of(ModsecOperator.MATCHES_REGEX);
        break;
      case MATCH_OPERATOR_CONTAINS:
        operator = Optional.of(ModsecOperator.CONTAINS);
        break;
      case MATCH_OPERATOR_GREATER_THAN:
        operator = Optional.of(ModsecOperator.GREATER_THAN);
        break;
      case MATCH_OPERATOR_LESS_THAN:
        operator = Optional.of(ModsecOperator.LESS_THAN);
        break;
      default:
        operator = Optional.empty();
    }
    return operator.map(op -> new ModsecOperatorExpression(op, false, expression.getMatchValue()));
  }

  ModsecOperatorExpression getOppositePositiveOperatorExpression(
      CustomModsecMatchExpression expression) {
    ModsecOperator operator;
    switch (expression.getValueMatchOperator()) {
      case MATCH_OPERATOR_NOT_EQUAL:
        operator = ModsecOperator.EQUALS;
        break;
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        operator = ModsecOperator.MATCHES_REGEX;
        break;
      case MATCH_OPERATOR_NOT_CONTAIN:
        operator = ModsecOperator.CONTAINS;
        break;
      default:
        throw new IllegalArgumentException(
            "No positive operator exists for operator: " + expression.getValueMatchOperator());
    }
    return new ModsecOperatorExpression(operator, false, expression.getMatchValue());
  }

  Optional<ModsecVariableMetadataKey.ModsecVariableKeyOperator> getVariableMetadataOperator(
      CustomModsecMatchExpression expression) {
    switch (expression.getValueMatchOperator()) {
      case MATCH_OPERATOR_EQUALS:
        return Optional.of(ModsecVariableMetadataKey.ModsecVariableKeyOperator.EQUALS);
      case MATCH_OPERATOR_MATCHES_REGEX:
        if (!expression.getMatchValue().contains(PIPE)) {
          return Optional.of(ModsecVariableMetadataKey.ModsecVariableKeyOperator.MATCHES_REGEX);
        }
      default:
        return Optional.empty();
    }
  }

  Optional<ModsecVariableMetadataKey.ModsecVariableKeyOperator>
      getOppositePositiveVariableMetadataOperator(CustomModsecMatchExpression expression) {
    switch (expression.getValueMatchOperator()) {
      case MATCH_OPERATOR_NOT_EQUAL:
        return Optional.of(ModsecVariableMetadataKey.ModsecVariableKeyOperator.EQUALS);
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        if (!expression.getMatchValue().contains(PIPE)) {
          // PIPE in Variable Regex is not supported, need to go the chained-rule route for that
          // ref: https://github.com/SpiderLabs/ModSecurity/issues/1591#issuecomment-337262698
          return Optional.of(ModsecVariableMetadataKey.ModsecVariableKeyOperator.MATCHES_REGEX);
        }
      default:
        return Optional.empty();
    }
  }
}
