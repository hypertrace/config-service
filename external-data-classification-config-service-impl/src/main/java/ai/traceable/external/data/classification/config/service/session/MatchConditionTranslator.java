package ai.traceable.external.data.classification.config.service.session;

import ai.traceable.external.data.classification.config.service.v1.Operator;
import ai.traceable.external.data.classification.config.service.v1.StringPredicate;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.MatchOperator;
import io.grpc.Status;

public class MatchConditionTranslator {

  public StringPredicate translate(MatchCondition matchCondition) {
    switch (matchCondition.getMatchValue().getValueCase()) {
      case STRING_VALUE:
        return buildStringPredicate(
            matchCondition.getOperator(), matchCondition.getMatchValue().getStringValue());
      case VALUE_NOT_SET:
        return buildStringPredicateForUnsetValue(matchCondition.getOperator());
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid match value present %s for string predicate",
                    matchCondition.getMatchValue()))
            .asRuntimeException();
    }
  }

  private StringPredicate buildStringPredicate(MatchOperator operator, String value) {
    switch (operator) {
      case MATCH_OPERATOR_EQUALS:
        return StringPredicate.newBuilder()
            .setOperator(Operator.OPERATOR_EQUALS)
            .setValue(value)
            .build();
      case MATCH_OPERATOR_CONTAINS:
        return StringPredicate.newBuilder()
            .setOperator(Operator.OPERATOR_MATCHES_REGEX)
            .setValue(escapeRegex(value))
            .build();
      case MATCH_OPERATOR_STARTS_WITH:
        return StringPredicate.newBuilder()
            .setOperator(Operator.OPERATOR_MATCHES_REGEX)
            .setValue("^" + escapeRegex(value))
            .build();
      case MATCH_OPERATOR_MATCHES_REGEX:
        return StringPredicate.newBuilder()
            .setOperator(Operator.OPERATOR_MATCHES_REGEX)
            .setValue(value)
            .build();
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(String.format("Unable to convert match operator %s", operator))
            .asRuntimeException();
    }
  }

  private StringPredicate buildStringPredicateForUnsetValue(MatchOperator operator) {
    if (operator == MatchOperator.MATCH_OPERATOR_NOT_EQUALS) {
      return StringPredicate.newBuilder()
          .setOperator(Operator.OPERATOR_MATCHES_REGEX)
          .setValue(".")
          .build();
    }
    throw Status.INVALID_ARGUMENT
        .withDescription(String.format("Unable to convert match operator %s", operator))
        .asRuntimeException();
  }

  private String escapeRegex(String value) {
    return value.replaceAll("\\W", "\\\\$0");
  }
}
