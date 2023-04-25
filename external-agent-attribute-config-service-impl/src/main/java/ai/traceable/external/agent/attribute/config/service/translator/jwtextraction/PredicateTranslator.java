package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.URL_KEYS;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector;
import ai.traceable.jwt.extraction.config.service.v1.Predicate;
import ai.traceable.jwt.extraction.config.service.v1.StringPredicate;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(onConstructor_ = @Inject)
class PredicateTranslator {
  private final AttributeRuleBuilder attributeRuleBuilder;
  private final TranslationUtils translationUtils;

  public ConditionalProjector.Predicate translatePredicate(Predicate predicate)
      throws JwtTranslationException {
    switch (predicate.getPredicateCase()) {
      case URL_PREDICATE:
        return translateUrlPredicate(predicate.getUrlPredicate());
      case COMPOSITE_PREDICATE:
        return translateCompositePredicate(predicate.getCompositePredicate());
      default:
        throw new JwtTranslationException(
            "Unknown jwt predicate case " + predicate.getPredicateCase());
    }
  }

  private ConditionalProjector.Predicate translateCompositePredicate(
      Predicate.CompositePredicate compositePredicate) throws JwtTranslationException {
    List<ConditionalProjector.Predicate> translatedChildrenPredicates = new ArrayList<>();
    for (Predicate childPredicate : compositePredicate.getChildrenList()) {
      translatedChildrenPredicates.add(translatePredicate(childPredicate));
    }
    return ConditionalProjector.Predicate.newBuilder()
        .setLogicalPredicate(
            ConditionalProjector.Predicate.LogicalPredicate.newBuilder()
                .setOperator(translationUtils.convert(compositePredicate.getOperator()))
                .addAllChildren(translatedChildrenPredicates))
        .build();
  }

  private ConditionalProjector.Predicate translateUrlPredicate(StringPredicate urlPredicate)
      throws JwtTranslationException {
    ConditionalProjector.Predicate.ComparisonOperator convertedOperator =
        translationUtils.convert(urlPredicate.getOperator());
    return attributeRuleBuilder.buildPredicate(
        ConditionalProjector.Predicate.ComparisonOperator.COMPARISON_OPERATOR_EQUALS,
        URL_KEYS,
        convertedOperator,
        urlPredicate.getValue());
  }
}
