package ai.traceable.external.agent.attribute.config.service.translator.userattributionv2;

import static ai.traceable.config.utils.RegexUtils.escapeRegex;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.ComparisonOperator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.StringPredicate;
import ai.traceable.userattribution.config.service.v2.KeyMatch;
import ai.traceable.userattribution.config.service.v2.KeyMatchOperator;
import ai.traceable.userattribution.config.service.v2.MatchCondition;
import ai.traceable.userattribution.config.service.v2.ValueMatchOperator;
import io.grpc.Status;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
class MatchConditionTranslator {
  StringPredicate translate(MatchCondition matchCondition) {
    switch (matchCondition.getMatchValue().getValueCase()) {
      case STRING_VALUE:
        return buildStringPredicate(
            matchCondition.getOperator(), matchCondition.getMatchValue().getStringValue());
      case NULL_VALUE:
        return buildStringPredicateForNullValue(matchCondition.getOperator());
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid match value present %s for name predicate",
                    matchCondition.getMatchValue()))
            .asRuntimeException();
    }
  }

  StringPredicate translate(KeyMatch keyMatch) {
    return buildStringPredicate(keyMatch.getOperator(), keyMatch.getMatchKey());
  }

  StringPredicate buildStringPredicate(ValueMatchOperator operator, String value) {
    switch (operator) {
      case VALUE_MATCH_OPERATOR_EQUALS:
        return StringPredicate.newBuilder()
            .setOperator(ComparisonOperator.COMPARISON_OPERATOR_EQUALS)
            .setValue(value)
            .build();
      case VALUE_MATCH_OPERATOR_NOT_EQUALS:
        return StringPredicate.newBuilder()
            .setOperator(ComparisonOperator.COMPARISON_OPERATOR_NOT_EQUALS)
            .setValue(value)
            .build();
      case VALUE_MATCH_OPERATOR_CONTAINS:
        return StringPredicate.newBuilder()
            .setOperator(ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX)
            .setValue(escapeRegex(value))
            .build();
      case VALUE_MATCH_OPERATOR_STARTS_WITH:
        return StringPredicate.newBuilder()
            .setOperator(ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX)
            .setValue("^" + escapeRegex(value))
            .build();
      case VALUE_MATCH_OPERATOR_MATCHES_REGEX:
        return StringPredicate.newBuilder()
            .setOperator(ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX)
            .setValue(value)
            .build();
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(String.format("Unable to convert match operator %s", operator))
            .asRuntimeException();
    }
  }

  private StringPredicate buildStringPredicateForNullValue(ValueMatchOperator operator) {
    if (operator == ValueMatchOperator.VALUE_MATCH_OPERATOR_NOT_EQUALS) {
      return StringPredicate.newBuilder()
          .setOperator(ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX)
          .setValue(".")
          .build();
    }
    throw Status.INVALID_ARGUMENT
        .withDescription(String.format("Unable to convert match operator %s", operator))
        .asRuntimeException();
  }

  StringPredicate buildStringPredicate(KeyMatchOperator operator, String value) {
    switch (operator) {
      case KEY_MATCH_OPERATOR_EQUALS:
        return StringPredicate.newBuilder()
            .setOperator(ComparisonOperator.COMPARISON_OPERATOR_EQUALS)
            .setValue(value)
            .build();
      case KEY_MATCH_OPERATOR_CONTAINS:
        return StringPredicate.newBuilder()
            .setOperator(ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX)
            .setValue(escapeRegex(value))
            .build();
      case KEY_MATCH_OPERATOR_STARTS_WITH:
        return StringPredicate.newBuilder()
            .setOperator(ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX)
            .setValue("^" + escapeRegex(value))
            .build();
      case KEY_MATCH_OPERATOR_MATCHES_REGEX:
        return StringPredicate.newBuilder()
            .setOperator(ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX)
            .setValue(value)
            .build();
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(String.format("Unable to convert match operator %s", operator))
            .asRuntimeException();
    }
  }
}
