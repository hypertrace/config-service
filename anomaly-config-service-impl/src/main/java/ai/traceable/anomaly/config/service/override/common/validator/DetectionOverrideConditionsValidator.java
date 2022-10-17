package ai.traceable.anomaly.config.service.override.common.validator;

import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideConditionsConfig;
import ai.traceable.anomaly.config.service.v1.override.KeyMatchCondition;
import ai.traceable.anomaly.config.service.v1.override.KeyMetadata;
import ai.traceable.anomaly.config.service.v1.override.KeyValueMatchCondition;
import ai.traceable.anomaly.config.service.v1.override.MatchCondition;
import ai.traceable.anomaly.config.service.v1.override.MatchConditionClause;
import ai.traceable.anomaly.config.service.v1.override.MatchConditionClauseOperator;
import ai.traceable.anomaly.config.service.v1.override.MatchConditionsClauseGroup;
import ai.traceable.anomaly.config.service.v1.override.MatchOperator;
import ai.traceable.anomaly.config.service.v1.override.MatchValue;
import ai.traceable.config.utils.RegexValidator;
import io.grpc.Status;
import java.util.List;
import java.util.function.Predicate;
import lombok.NonNull;

public class DetectionOverrideConditionsValidator {

  Status validateConditionsConfig(@NonNull DetectionOverrideConditionsConfig config) {
    if (!config.hasMatchConditionsClauseGroup()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Conditions Config should have a match condition clause group");
    }
    return validateMatchConditionsClauseGroup(config.getMatchConditionsClauseGroup());
  }

  private Status validateMatchConditionsClauseGroup(
      @NonNull MatchConditionsClauseGroup matchConditionsClauseGroup) {
    if (matchConditionsClauseGroup
        .getClauseType()
        .equals(MatchConditionClauseOperator.MATCH_CONDITION_CLAUSE_OPERATOR_UNSPECIFIED)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Match condition clause operator in match condition clause group should have a valid operator");
    }

    return validateConditions(matchConditionsClauseGroup.getConditionsList());
  }

  private Status validateConditions(List<MatchConditionClause> clauseList) {
    if (clauseList.isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Conditions should have at least one match condition clause");
    }

    for (MatchConditionClause conditionClause : clauseList) {
      switch (conditionClause.getConditionCase()) {
        case KEY_VALUE_CONDITION:
          Status status = validateKeyValueMatchCondition(conditionClause.getKeyValueCondition());
          if (status != Status.OK) {
            return status;
          }
          break;
        default:
          return Status.INVALID_ARGUMENT.withDescription(
              String.format(
                  "Invalid case of match condition clause %s", conditionClause.getConditionCase()));
      }
    }

    return Status.OK;
  }

  private Status validateKeyValueMatchCondition(@NonNull KeyValueMatchCondition condition) {
    if (!condition.hasKeyMatchCondition() || !condition.hasValueMatchCondition()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "KeyValueMatchCondition should have a key/value match condition");
    }

    Status status;
    if ((status = validateKeyMatchCondition(condition.getKeyMatchCondition())) != Status.OK) {
      return status;
    }
    if ((status = validateMatchCondition(condition.getValueMatchCondition())) != Status.OK) {
      return status;
    }
    return Status.OK;
  }

  private Status validateKeyMatchCondition(@NonNull KeyMatchCondition condition) {
    if (condition.getMetadata().equals(KeyMetadata.KEY_METADATA_UNSPECIFIED)) {
      return Status.INVALID_ARGUMENT.withDescription("Metadata should have a valid key metadata");
    }

    if (condition.hasMatchCondition()) {
      return validateMatchCondition(condition.getMatchCondition());
    }

    return Status.OK;
  }

  private Status validateMatchCondition(@NonNull MatchCondition condition) {
    if (condition.getOperator().equals(MatchOperator.MATCH_OPERATOR_UNSPECIFIED)) {
      return Status.INVALID_ARGUMENT.withDescription("Operator should have a valid match operator");
    }

    if (!condition.hasValue()) {
      return Status.INVALID_ARGUMENT.withDescription("MatchCondition should have a match value");
    }
    return validateMatchValue(
        condition.getValue(),
        condition.getOperator() == MatchOperator.MATCH_OPERATOR_MATCHES_REGEX
            || condition.getOperator() == MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX);
  }

  private Status validateMatchValue(@NonNull MatchValue value, boolean isRegexMatched) {
    switch (value.getValueCase()) {
      case STRING_VALUE:
        if (value.getStringValue().isEmpty()) {
          return Status.INVALID_ARGUMENT.withDescription(
              "Value shouldn't have an empty string value");
        }
        if (isRegexMatched) {
          return RegexValidator.validate(value.getStringValue());
        }
        break;
      case LIST_VALUE:
        if (value.getListValue().getValuesList().isEmpty()) {
          return Status.INVALID_ARGUMENT.withDescription(
              "Value list should have at least one value");
        }
        if (isRegexMatched) {
          return value.getListValue().getValuesList().stream()
              .map(RegexValidator::validate)
              .filter(Predicate.not(Status::isOk))
              .findFirst()
              .orElse(Status.OK);
        }
        break;
      default:
        return Status.INVALID_ARGUMENT.withDescription(
            String.format("Invalid case of match value %s", value.getValueCase()));
    }
    return Status.OK;
  }
}
