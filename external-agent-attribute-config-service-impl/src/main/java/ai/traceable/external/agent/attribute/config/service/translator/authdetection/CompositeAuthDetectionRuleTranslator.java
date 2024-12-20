package ai.traceable.external.agent.attribute.config.service.translator.authdetection;

import ai.traceable.auth.detection.config.service.v1.Predicate.PredicateCase;
import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class CompositeAuthDetectionRuleTranslator extends AuthDetectionRuleTranslator {
  private final PredicateBuilder predicateBuilder;

  @Inject
  CompositeAuthDetectionRuleTranslator(
      AttributeRuleBuilder attributeRuleBuilder, PredicateBuilder predicateBuilder) {
    super(attributeRuleBuilder);
    this.predicateBuilder = predicateBuilder;
  }

  @Override
  public PredicateCase getPredicateCase() {
    return PredicateCase.COMPOSITE_PREDICATE;
  }

  @Override
  public Optional<Predicate> translatePredicate(
      ai.traceable.auth.detection.config.service.v1.Predicate predicate) {
    return this.predicateBuilder.translateCompositePredicate(predicate.getCompositePredicate());
  }
}
