package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.ComparisonOperator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.LogicalOperator;
import ai.traceable.jwt.extraction.config.service.v1.Predicate;
import ai.traceable.jwt.extraction.config.service.v1.StringPredicate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class TranslationUtils {
  public ComparisonOperator convert(StringPredicate.RelationalOperator relationalOperator)
      throws JwtTranslationException {
    switch (relationalOperator) {
      case RELATIONAL_OPERATOR_EQUALS:
        return ComparisonOperator.COMPARISON_OPERATOR_EQUALS;
      case RELATIONAL_OPERATOR_NOT_EQUALS:
        return ComparisonOperator.COMPARISON_OPERATOR_NOT_EQUALS;
      case RELATIONAL_OPERATOR_MATCHES_REGEX:
        return ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX;
      case RELATIONAL_OPERATOR_NOT_MATCHES_REGEX:
        return ComparisonOperator.COMPARISON_OPERATOR_NOT_MATCHES_REGEX;
      default:
        throw new JwtTranslationException(
            String.format("Mapping comparison operator not defined for %s", relationalOperator));
    }
  }

  public LogicalOperator convert(
      Predicate.CompositePredicate.LogicalOperator compositePredicateOperator)
      throws JwtTranslationException {
    switch (compositePredicateOperator) {
      case LOGICAL_OPERATOR_OR:
        return LogicalOperator.LOGICAL_OPERATOR_OR;
      case LOGICAL_OPERATOR_AND:
        return LogicalOperator.LOGICAL_OPERATOR_AND;
      default:
        throw new JwtTranslationException(
            String.format(
                "Mapping logical operator not defined for %s", compositePredicateOperator));
    }
  }
}
